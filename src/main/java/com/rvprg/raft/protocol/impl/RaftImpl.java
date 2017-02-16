package com.rvprg.raft.protocol.impl;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse.Builder;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageReceiver;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import net.jcip.annotations.GuardedBy;

public class RaftImpl implements Raft {
    Logger logger = LoggerFactory.getLogger(RaftImpl.class);

    private final MemberConnector memberConnector;
    private final MessageReceiver messageReceiver;

    // TODO: Make debug messages consistent format.
    // FIXME: Use configuration object instead?
    private final long heartbeatTimeout;
    private final int heartbeatPeriod;
    private final int electionMinTimeout;
    private final int electionMaxTimeout;

    private final EventLoopGroup eventLoop;

    private final AtomicReference<ScheduledFuture<?>> newElectionInitiatorTask = new AtomicReference<ScheduledFuture<?>>();
    private final AtomicReference<ScheduledFuture<?>> electionTimeoutMonitorTask = new AtomicReference<ScheduledFuture<?>>();
    private final AtomicReference<ScheduledFuture<?>> periodicHeartbeatTask = new AtomicReference<ScheduledFuture<?>>();

    private final RaftObserver observer;

    private final Object stateLock = new Object();
    @GuardedBy("stateLock")
    private int currentTerm = 0;
    @GuardedBy("stateLock")
    private int votesReceived = 0;
    @GuardedBy("stateLock")
    private MemberId votedFor = null;
    @GuardedBy("stateLock")
    private MemberId leader;
    private Role role = Role.Follower;

    private final MemberId selfId;

    private final Random random = new Random();

    private final Log log;

    private final AtomicBoolean started = new AtomicBoolean(false);

    @Inject
    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, RaftObserver observer) {
        this(configuration, memberConnector, messageReceiver, log, 0, Role.Follower, observer);
    }

    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, int initTerm, Role initRole, RaftObserver observer) {
        // FIXME: check messageReceiver is not started.
        this.heartbeatTimeout = configuration.getHeartbeatTimeout();
        this.memberConnector = memberConnector;
        this.messageReceiver = messageReceiver;
        this.observer = observer;
        this.selfId = messageReceiver.getMemberId();
        this.log = log;
        this.currentTerm = initTerm;
        this.role = initRole;
        this.leader = null;
        this.heartbeatPeriod = configuration.getHeartbeatPeriod();
        this.electionMinTimeout = configuration.getElectionMinTimeout();
        this.electionMaxTimeout = configuration.getElectionMaxTimeout();
        this.eventLoop = new NioEventLoopGroup();

        MemberConnectorObserverImpl memberConnectorObserver = new MemberConnectorObserverImpl(this, messageReceiver.getChannelPipelineInitializer());
        configuration.getMembers().forEach(member -> memberConnector.register(member, memberConnectorObserver));
    }

    @Override
    public void start() throws InterruptedException {
        // FIXME: Check if started.
        messageReceiver.start(this);
        memberConnector.connectAllRegistered();
        scheduleHeartbeatMonitorTask();
        started.set(true);
        observer.started();
    }

    @Override
    public void shutdown() {
        cancelHeartbeatMonitorTask();
        cancelElectionTimeoutTask();
        messageReceiver.shutdown();
        eventLoop.shutdownGracefully().awaitUninterruptibly();
        started.set(false);
        observer.shutdown();
    }

    @Override
    public void consumeRequestVote(Channel senderChannel, RequestVote requestVote) {
        boolean ignoreMessage = checkTermRecency(requestVote.getTerm());
        if (ignoreMessage) {
            return;
        }
        Builder response = RequestVoteResponse.newBuilder();

        boolean grantVote = false;

        synchronized (stateLock) {
            response.setTerm(getCurrentTerm());
            MemberId candidateId = MemberId.fromString(requestVote.getCandidateId());
            if (requestVote.getTerm() < getCurrentTerm()) {
                grantVote = false;
            } else if (votedFor == null || votedFor.equals(candidateId)) {
                if (requestVote.getTerm() == getCurrentTerm()) {
                    grantVote = checkCandidatesLogIsUpToDate(requestVote);
                }
            }

            if (grantVote) {
                votedFor = candidateId;
                logger.debug("Member: {}. Term: {}. Giving vote to: {}.", selfId, currentTerm, votedFor);
            }
        }

        RaftMessage responseMessage = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(grantVote).build())
                .build();
        senderChannel.writeAndFlush(responseMessage);
    }

    @Override
    public void consumeRequestVoteResponse(Channel senderChannel, RequestVoteResponse requestVoteResponse) {
        boolean ignoreMessage = checkTermRecency(requestVoteResponse.getTerm());
        if (ignoreMessage) {
            observer.voteRejected();
            return;
        }

        if (getRole() != Role.Candidate) {
            return;
        }

        synchronized (stateLock) {
            boolean sameTerm = requestVoteResponse.getTerm() == getCurrentTerm();

            if (sameTerm && requestVoteResponse.getVoteGranted()) {
                observer.voteReceived();
                ++votesReceived;
                logger.debug("Member: {}. Term: {}. Votes received: {}.", selfId, currentTerm, votesReceived);
            } else {
                observer.voteRejected();
            }

            if (votesReceived >= getMajority()) {
                becomeLeader();
                observer.electionWon();
            }
        }
    }

    private boolean checkCandidatesLogIsUpToDate(RequestVote requestVote) {
        if (log.getLast().getTerm() == requestVote.getLastLogTerm()) {
            return requestVote.getLastLogIndex() >= log.length();
        }
        return requestVote.getLastLogTerm() >= log.getLast().getTerm();
    }

    private int compareAndUpdateCurrentTerm(int term) {
        synchronized (stateLock) {
            if (term > currentTerm) {
                currentTerm = term;
                return 1;
            } else if (term < currentTerm) {
                return -1;
            }
        }
        return 0;
    }

    private void becomeLeader() {
        synchronized (stateLock) {
            if (getRole() != Role.Candidate) {
                return;
            }
            logger.debug("Member: {}. Term: {}. Votes Received: {}. BECAME LEADER.", selfId, currentTerm, votesReceived);
            cancelElectionTimeoutTask();
            scheduleSendHeartbeats();
            schedulePeriodicHeartbeatTask();
            role = Role.Leader;
            leader = selfId;
            votesReceived = 0;
        }
    }

    private void becomeFollower() {
        cancelElectionTimeoutTask();
        cancelPeriodicHeartbeatTask();
        scheduleHeartbeatMonitorTask();
        synchronized (stateLock) {
            role = Role.Follower;
            votedFor = null;
            votesReceived = 0;
        }
    }

    private boolean checkTermRecency(int term) {
        int comparisonResult = compareAndUpdateCurrentTerm(term);
        if (comparisonResult == -1) {
            return true;
        }
        if (comparisonResult == 1) {
            becomeFollower();
        }
        return false;
    }

    @Override
    public void consumeAppendEntries(Channel senderChannel, AppendEntries appendEntries) {
        boolean ignoreMessage = checkTermRecency(appendEntries.getTerm());
        if (ignoreMessage) {
            return;
        }

        synchronized (stateLock) {
            if (appendEntries.hasLeaderId()) {
                MemberId otherLeader = MemberId.fromString(appendEntries.getLeaderId());
                if (leader != null &&
                        !leader.equals(otherLeader) &&
                        role != Role.Follower) {
                    becomeFollower();
                }
                leader = otherLeader;
            }
        }

        if (appendEntries.getLogEntriesCount() == 0) {
            processHeartbeat(appendEntries);
        }
    }

    @Override
    public void consumeAppendEntriesResponse(Channel senderChannel, AppendEntriesResponse appendEntriesResponse) {
        boolean ignoreMessage = checkTermRecency(appendEntriesResponse.getTerm());
        if (ignoreMessage) {
            return;
        }
    }

    private void processHeartbeat(AppendEntries heartbeat) {
        observer.heartbeatReceived();
        scheduleHeartbeatMonitorTask();
    }

    private void cancelTask(ScheduledFuture<?> task) {
        if (task != null) {
            task.cancel(true);
        }
    }

    private void cancelHeartbeatMonitorTask() {
        cancelTask(newElectionInitiatorTask.get());
    }

    private void cancelElectionTimeoutTask() {
        cancelTask(electionTimeoutMonitorTask.get());
    }

    private void cancelPeriodicHeartbeatTask() {
        cancelTask(periodicHeartbeatTask.get());
    }

    private void scheduleHeartbeatMonitorTask() {
        cancelTask(newElectionInitiatorTask.getAndSet(scheduleNextElectionTask()));
    }

    private ScheduledFuture<?> scheduleNextElectionTask() {
        observer.nextElectionScheduled();
        return this.eventLoop.schedule(() -> RaftImpl.this.heartbeatTimedout(), heartbeatTimeout, TimeUnit.MILLISECONDS);
    }

    private void scheduleElectionTimeoutTask() {
        final int timeout = random.nextInt(electionMaxTimeout - electionMinTimeout) + electionMinTimeout;
        ScheduledFuture<?> prevTask = electionTimeoutMonitorTask.getAndSet(
                this.eventLoop.schedule(() -> RaftImpl.this.electionTimedout(), timeout, TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private void schedulePeriodicHeartbeatTask() {
        ScheduledFuture<?> prevTask = periodicHeartbeatTask.getAndSet(
                this.eventLoop.scheduleAtFixedRate(() -> RaftImpl.this.scheduleSendHeartbeats(), 0, heartbeatPeriod, TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private int getMajority() {
        return (memberConnector.getRegisteredMemberIds().size() + 1) / 2 + 1;
    }

    private void electionTimedout() {
        observer.electionTimedout();
        synchronized (stateLock) {
            logger.debug("Member: {}, Term: {}, Election timedout.", selfId, currentTerm);
        }
        initiateElection();
    }

    private void heartbeatTimedout() {
        observer.heartbeatTimedout();
        initiateElection();
    }

    private void initiateElection() {
        cancelHeartbeatMonitorTask();
        scheduleElectionTimeoutTask();

        synchronized (stateLock) {
            role = Role.Candidate;
            ++currentTerm;
            votedFor = null;
            leader = null;
            votesReceived = 0;
            logger.debug("Member: {}, Term: {}, New election.", selfId, currentTerm);
        }

        synchronized (stateLock) {
            if (votedFor == null) {
                ++votesReceived;
                votedFor = selfId;
                logger.debug("Member: {}, Term: {}, Votes Received: {}. Voted for itself.", selfId, currentTerm, votesReceived);
            } else {
                return;
            }
        }

        scheduleSendVoteRequests();
    }

    private void scheduleSendMessageToEachMember(RaftMessage msg) {
        memberConnector.getActiveMembers().getAll().forEach(
                member -> this.eventLoop.schedule(() -> RaftImpl.this.sendMessage(member, msg), 0, TimeUnit.MILLISECONDS));
    }

    private void scheduleSendVoteRequests() {
        if (started.get()) {
            scheduleSendMessageToEachMember(getRequestVoteMessage());
        }
    }

    private void scheduleSendHeartbeats() {
        if (started.get()) {
            scheduleSendMessageToEachMember(getHeartbeatMessage());
        }
    }

    private RaftMessage getRequestVoteMessage() {
        RequestVote req = RequestVote.newBuilder()
                .setTerm(getCurrentTerm())
                .setCandidateId(selfId.toString())
                .setLastLogIndex(log.length())
                .setLastLogTerm(log.getLast().getTerm()).build();

        RaftMessage requestVote = RaftMessage.newBuilder()
                .setType(MessageType.RequestVote)
                .setRequestVote(req)
                .build();

        return requestVote;
    }

    private RaftMessage getHeartbeatMessage() {
        AppendEntries.Builder req = AppendEntries.newBuilder();

        synchronized (stateLock) {
            req.setTerm(getCurrentTerm());
            if (leader != null) {
                req.setLeaderId(leader.toString());
            }
        }

        RaftMessage requestVote = RaftMessage.newBuilder()
                .setType(MessageType.AppendEntries)
                .setAppendEntries(req.build())
                .build();

        return requestVote;
    }

    private void sendMessage(Member member, RaftMessage req) {
        Channel memberChannel = member.getChannel();
        if (memberChannel.isActive()) {
            if (req.getType() == RaftMessage.MessageType.RequestVote) {
                logger.debug("Member: {}, Term: {}. Vote request sent to {}.", selfId, getCurrentTerm(), member.getMemberId());
            }
            memberChannel.writeAndFlush(req);
        }
    }

    @Override
    public Role getRole() {
        synchronized (stateLock) {
            return role;
        }
    }

    @Override
    public int getCurrentTerm() {
        synchronized (stateLock) {
            return currentTerm;
        }
    }

}

package com.rvprg.raft.protocol.impl;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.messages.ProtocolMessages;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse.Builder;
import com.rvprg.raft.sm.StateMachine;
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
    private final Logger logger = LoggerFactory.getLogger(RaftImpl.class);

    private final MemberId selfId;
    private final Configuration configuration;
    private final MemberConnector memberConnector;
    private final MessageReceiver messageReceiver;
    private final Log log;

    // TODO: Make debug messages consistent format.
    private final AtomicReference<ScheduledFuture<?>> newElectionInitiatorTask = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> electionTimeoutMonitorTask = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> periodicHeartbeatTask = new AtomicReference<>();

    private final ConcurrentHashMap<Integer, CompletableFuture<Integer>> replicationCompletableFutures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MemberId, AtomicReference<ScheduledFuture<?>>> replicationRetryTasks = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock indexesLock = new ReentrantReadWriteLock();
    @GuardedBy("indexesLock")
    private final ConcurrentHashMap<MemberId, AtomicInteger> nextIndexes = new ConcurrentHashMap<>();
    @GuardedBy("indexesLock")
    private final ConcurrentHashMap<MemberId, AtomicInteger> matchIndexes = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<MemberId, ConcurrentHashMap<Long, AppendEntriesRequestMeta>> appendEntriesRequestMetas = new ConcurrentHashMap<MemberId, ConcurrentHashMap<Long, AppendEntriesRequestMeta>>();

    private static class AppendEntriesRequestMeta {
        public final int nextIndex;
        public final int logEntriesLength;

        public AppendEntriesRequestMeta(int nextIndex, int logEntriesLength) {
            this.nextIndex = nextIndex;
            this.logEntriesLength = logEntriesLength;
        }
    }

    private final AtomicLong nextRequestSequenceNumber = new AtomicLong(0L);

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
    @GuardedBy("stateLock")
    private Role role = Role.Follower;
    @GuardedBy("stateLock")
    private boolean started = false;
    @GuardedBy("stateLock")
    private AtomicReference<EventLoopGroup> eventLoop = new AtomicReference<EventLoopGroup>(null);

    private final Random random;

    private final StateMachine stateMachine;

    @Inject
    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, StateMachine stateMachine, RaftObserver observer) {
        this(configuration, memberConnector, messageReceiver, log, stateMachine, 0, Role.Follower, observer);
    }

    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, StateMachine stateMachine,
            int initTerm, Role initRole,
            RaftObserver observer) {
        this.memberConnector = memberConnector;
        this.messageReceiver = messageReceiver;
        this.selfId = messageReceiver.getMemberId();
        this.log = log;
        this.currentTerm = initTerm;
        this.role = initRole;
        this.leader = null;
        this.configuration = configuration;
        this.random = new Random();
        this.observer = observer == null ? RaftObserver.getDefaultInstance() : observer;
        this.stateMachine = stateMachine;

        // FIXME: double initialization
        initializeEventLoop();

        MemberConnectorObserverImpl memberConnectorObserver = new MemberConnectorObserverImpl(this, messageReceiver.getChannelPipelineInitializer());
        configuration.getMemberIds().forEach(memberId -> memberConnector.register(memberId, memberConnectorObserver));
    }

    private void initializeEventLoop() {
        EventLoopGroup prevEventLoop = eventLoop.getAndSet(new NioEventLoopGroup());
        if (prevEventLoop != null) {
            prevEventLoop.shutdownGracefully();
        }
    }

    @Override
    public void start() throws InterruptedException {
        synchronized (stateLock) {
            initializeEventLoop();
            messageReceiver.start(this);
            memberConnector.connectAllRegistered();
            scheduleHeartbeatMonitorTask();
            started = true;
            observer.started();
            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> replicationRetryTasks.put(memberId, new AtomicReference<ScheduledFuture<?>>(null)));
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        synchronized (stateLock) {
            cancelHeartbeatMonitorTask();
            cancelElectionTimeoutTask();
            messageReceiver.shutdown();
            eventLoop.get().shutdownGracefully();
            started = false;
            observer.shutdown();
        }
    }

    @Override
    public void consumeRequestVote(Member member, RequestVote requestVote) {
        boolean ignoreMessage = checkTermRecency(requestVote.getTerm());
        if (ignoreMessage) {
            return;
        }
        Builder response = RequestVoteResponse.newBuilder();

        boolean grantVote = false;

        synchronized (stateLock) {
            response.setTerm(getCurrentTerm());
            MemberId candidateId = MemberId.fromString(requestVote.getCandidateId());
            if (votedFor == null || votedFor.equals(candidateId)) {
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
        member.getChannel().writeAndFlush(responseMessage);
    }

    @Override
    public void consumeRequestVoteResponse(Member member, RequestVoteResponse requestVoteResponse) {
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
                observer.electionWon(currentTerm);
            }
        }
    }

    private boolean checkCandidatesLogIsUpToDate(RequestVote requestVote) {
        if (log.getLast().getTerm() == requestVote.getLastLogTerm()) {
            return requestVote.getLastLogIndex() >= log.getLastIndex();
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
            int lastLogIndex = log.getLastIndex();
            nextRequestSequenceNumber.set(0);

            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> {
                        nextIndexes.put(memberId, new AtomicInteger(lastLogIndex + 1));
                        matchIndexes.put(memberId, new AtomicInteger(0));
                        replicationRetryTasks.putIfAbsent(memberId, new AtomicReference<ScheduledFuture<?>>(null));
                        appendEntriesRequestMetas.putIfAbsent(memberId, new ConcurrentHashMap<>());
                        appendEntriesRequestMetas.get(memberId).clear();
                    });
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
            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> {
                        cancelTask(replicationRetryTasks.get(memberId).getAndSet(null));
                    });
            appendEntriesRequestMetas.forEach((member, metas) -> metas.clear());
        }
        clearIndexes();
    }

    private void clearIndexes() {
        indexesLock.writeLock().lock();
        try {
            nextIndexes.clear();
            matchIndexes.clear();
        } finally {
            indexesLock.writeLock().unlock();
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
    public void consumeAppendEntries(Member member, AppendEntries appendEntries) {
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
        } else {
            boolean successFlag = processAppendEntries(appendEntries);
            sendAppendEntriesResponse(member, appendEntries.getSequenceNumber(), successFlag);
        }
    }

    private void sendAppendEntriesResponse(Member member, long seq, boolean success) {
        AppendEntriesResponse.Builder response = AppendEntriesResponse.newBuilder();
        RaftMessage responseMessage = RaftMessage.newBuilder()
                .setType(MessageType.AppendEntriesResponse)
                .setAppendEntriesResponse(response.setSuccess(success).setTerm(getCurrentTerm()).setSequenceNumber(seq).build())
                .build();
        member.getChannel().writeAndFlush(responseMessage);
    }

    private boolean processAppendEntries(AppendEntries appendEntries) {
        List<LogEntry> logEntries = appendEntries.getLogEntriesList()
                .stream().map(x -> new LogEntry(x.getTerm(), x.getEntry().asReadOnlyByteBuffer()))
                .collect(Collectors.toList());
        return log.append(appendEntries.getPrevLogIndex(), appendEntries.getPrevLogTerm(), logEntries);
    }

    @Override
    public void consumeAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse) {
        boolean ignoreMessage = checkTermRecency(appendEntriesResponse.getTerm());
        if (ignoreMessage) {
            return;
        }

        boolean isAccepted = appendEntriesResponse.getSuccess();

        if (isAccepted) {
            checkReplicationStatus();
            AppendEntriesRequestMeta meta = appendEntriesRequestMetas.get(member.getMemberId()).remove(appendEntriesResponse.getSequenceNumber());
            if (meta != null) {
                indexesLock.writeLock().lock();
                try {
                    if (nextIndexes.get(member.getMemberId()).get() != meta.nextIndex) {
                        return;
                    }

                    int newNextIndex = meta.nextIndex + meta.logEntriesLength + 1;
                    int newMatchIndex = meta.nextIndex + meta.logEntriesLength;

                    nextIndexes.get(member.getMemberId()).set(newNextIndex);
                    matchIndexes.get(member.getMemberId()).set(newMatchIndex);
                    // TODO: Cancel replication retry task?
                } finally {
                    indexesLock.writeLock().unlock();
                }
            } else {
                logger.error("Member: {}, Term: {}, got a response to a message we didn't send", member.getMemberId(), currentTerm);
            }
        } else {
            MemberId senderMemberId = member.getMemberId();
            indexesLock.writeLock().lock();
            try {
                nextIndexes.get(senderMemberId).decrementAndGet();
            } finally {
                indexesLock.writeLock().unlock();
            }
            scheduleAppendEntries(senderMemberId);
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
        return eventLoop.get().schedule(() -> RaftImpl.this.heartbeatTimedout(), configuration.getHeartbeatTimeout(), TimeUnit.MILLISECONDS);
    }

    private void scheduleElectionTimeoutTask() {
        final int timeout = random.nextInt(configuration.getElectionMaxTimeout() - configuration.getElectionMinTimeout()) + configuration.getElectionMinTimeout();
        ScheduledFuture<?> prevTask = electionTimeoutMonitorTask.getAndSet(
                eventLoop.get().schedule(() -> RaftImpl.this.electionTimedout(), timeout, TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private void schedulePeriodicHeartbeatTask() {
        ScheduledFuture<?> prevTask = periodicHeartbeatTask.getAndSet(
                eventLoop.get().scheduleAtFixedRate(() -> RaftImpl.this.scheduleSendHeartbeats(), 0, configuration.getHeartbeatPeriod(), TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    public CompletableFuture<Integer> applyCommand(ByteBuffer command) {
        // TODO: If not leader, re-direct.
        LogEntry logEntry = new LogEntry(getCurrentTerm(), command);
        return scheduleReplication(log.append(logEntry));
    }

    private CompletableFuture<Integer> scheduleReplication(int index) {
        CompletableFuture<Integer> future = new CompletableFuture<Integer>();
        replicationCompletableFutures.put(index, future);

        for (MemberId memberId : memberConnector.getRegisteredMemberIds()) {
            int nextIndex = 0;
            indexesLock.readLock().lock();
            try {
                nextIndex = nextIndexes.get(memberId).get();
            } finally {
                indexesLock.readLock().unlock();
            }
            if (nextIndex < log.getLastIndex()) {
                scheduleAppendEntries(memberId);
            }
        }

        return future;
    }

    private void scheduleAppendEntriesRetry(MemberId memberId) {
        try {
            ScheduledFuture<?> future = eventLoop.get().schedule(() -> this.scheduleAppendEntries(memberId), configuration.getReplicationRetryInterval(), TimeUnit.MILLISECONDS);
            cancelTask(replicationRetryTasks.get(memberId).getAndSet(future));
        } catch (RejectedExecutionException e) {
            logger.error("Member: {}, Term: {}, scheduling replication retry failed.", memberId, currentTerm, e);
        }
    }

    private void scheduleAppendEntries(MemberId memberId) {
        scheduleAppendEntriesRetry(memberId);
        Member member = memberConnector.getActiveMembers().get(memberId);
        if (member != null) {
            scheduleSendMessageToMember(member, getAppendEntries(memberId));
        }
    }

    private void checkReplicationStatus() {
        Iterator<Entry<Integer, CompletableFuture<Integer>>> it = replicationCompletableFutures.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, CompletableFuture<Integer>> entry = it.next();
            int currIndex = entry.getKey();

            int replicationCount = 0;
            for (AtomicInteger matchIndex : matchIndexes.values()) {
                indexesLock.readLock().lock();
                try {
                    if (matchIndex.get() >= currIndex) {
                        replicationCount++;
                    }
                } finally {
                    indexesLock.readLock().unlock();
                }
            }

            if (replicationCount >= getMajority()) {
                entry.getValue().complete(replicationCount);
                it.remove();
            }
        }
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
        memberConnector.getActiveMembers().getAll().forEach(member -> scheduleSendMessageToMember(member, msg));
    }

    private ScheduledFuture<?> scheduleSendMessageToMember(Member member, RaftMessage msg) {
        try {
            return member.getChannel().eventLoop().schedule(() -> RaftImpl.this.sendMessage(member, msg), 0, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.error("Member: {}, Term: {}, Message type: {}, message send failed.", member, currentTerm, msg.getType(), e);
        }
        return null;
    }

    private void scheduleSendVoteRequests() {
        scheduleSendMessageToEachMember(getRequestVoteMessage());
    }

    private void scheduleSendHeartbeats() {
        scheduleSendMessageToEachMember(getHeartbeatMessage());
    }

    private RaftMessage getAppendEntries(MemberId memberId) {
        int nextIndex = 0;
        indexesLock.readLock().lock();
        try {
            nextIndex = nextIndexes.get(memberId).get();
        } finally {
            indexesLock.readLock().unlock();
        }
        int prevLogIndex = nextIndex - 1;
        int prevLogTerm = log.get(prevLogIndex).getTerm();
        int commitIndex = log.getCommitIndex();
        long requestSequenceNumber = nextRequestSequenceNumber.incrementAndGet();

        AppendEntries.Builder req = AppendEntries.newBuilder()
                .setTerm(getCurrentTerm())
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommitIndex(commitIndex)
                .setSequenceNumber(requestSequenceNumber)
                .setLeaderId(leader.toString());

        log.get(nextIndex, configuration.getMaxNumberOfLogEntriesPerRequest())
                .forEach(logEntry -> req.addLogEntries(ProtocolMessages.LogEntry.newBuilder()
                        .setTerm(logEntry.getTerm())
                        .setEntry(ByteString.copyFrom(logEntry.getCommand()))));

        appendEntriesRequestMetas.get(memberId).put(
                requestSequenceNumber,
                new AppendEntriesRequestMeta(nextIndex, req.getLogEntriesCount()));

        RaftMessage requestVote = RaftMessage.newBuilder()
                .setType(MessageType.AppendEntries)
                .setAppendEntries(req)
                .build();

        return requestVote;
    }

    private RaftMessage getRequestVoteMessage() {
        RequestVote req = RequestVote.newBuilder()
                .setTerm(getCurrentTerm())
                .setCandidateId(selfId.toString())
                .setLastLogIndex(log.getLastIndex())
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

    @Override
    public boolean isStarted() {
        synchronized (stateLock) {
            return started;
        }
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public MemberId getMemberId() {
        return selfId;
    }

}

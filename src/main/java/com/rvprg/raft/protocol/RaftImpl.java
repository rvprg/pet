package com.rvprg.raft.protocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.log.LogEntryFactory;
import com.rvprg.raft.log.LogException;
import com.rvprg.raft.log.SnapshotInstallException;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.DynamicMembershipChangeCommand;
import com.rvprg.raft.protocol.messages.ProtocolMessages.DynamicMembershipChangeCommand.CommandType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse.Builder;
import com.rvprg.raft.protocol.messages.ProtocolMessages.SnapshotDownloadRequest;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageReceiver;
import com.rvprg.raft.transport.SnapshotDescriptor;
import com.rvprg.raft.transport.SnapshotMetadata;
import com.rvprg.raft.transport.SnapshotReceiver;
import com.rvprg.raft.transport.SnapshotSender;
import com.rvprg.raft.transport.SnapshotTransferCompletedEvent;
import com.rvprg.raft.transport.SnapshotTransferConnectionClosedEvent;
import com.rvprg.raft.transport.SnapshotTransferConnectionOpenedEvent;
import com.rvprg.raft.transport.SnapshotTransferEvent;
import com.rvprg.raft.transport.SnapshotTransferExceptionThrownEvent;
import com.rvprg.raft.transport.SnapshotTransferStartedEvent;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import net.jcip.annotations.GuardedBy;

public class RaftImpl implements Raft {
    private final Logger logger = LoggerFactory.getLogger(RaftImpl.class);
    private final static Marker severe = MarkerFactory.getMarker("SEVERE");

    private final MemberId selfId;
    private final Configuration configuration;
    private final RaftMemberConnector memberConnector;
    private final MessageReceiver messageReceiver;
    private final Log log;

    private final AtomicReference<ScheduledFuture<?>> newElectionInitiatorTask = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> electionTimeoutMonitorTask = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> periodicHeartbeatTask = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<?>> logCompactionTask = new AtomicReference<>(CompletableFuture.completedFuture(true));

    private final ConcurrentHashMap<Long, ApplyCommandFuture> replicationCompletableFutures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MemberId, AtomicReference<ScheduledFuture<?>>> replicationRetryTasks = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock indexesLock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<MemberId, AtomicLong> nextIndexes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MemberId, AtomicLong> matchIndexes = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<MemberId, Channel> snapshotRecipients = new ConcurrentHashMap<>();

    private final SnapshotSender snapshotSender;

    private final Object snapshotInstallLock = new Object();
    @GuardedBy("snapshotInstallLock")
    private SnapshotReceiver snapshotReceiver = null;

    private final Object dynamicMembershipChangeLock = new Object();
    @GuardedBy("dynamicMembershipChangeLock")
    private ApplyCommandResult dynamicMembershipChangeInProgress = null;

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

    private final MemberConnectorObserverImpl memberConnectorObserver;

    private final Random random;

    private final StateMachine stateMachine;

    private final AtomicBoolean catchingUpMember = new AtomicBoolean(false);
    private final ChannelPipelineInitializer channelPipelineInitializer;

    @Inject
    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, StateMachine stateMachine, RaftObserver observer)
            throws InterruptedException, SnapshotInstallException, FileNotFoundException, IOException, LogException {
        this(configuration, memberConnector, messageReceiver, log, stateMachine, log.getTerm(), Role.Follower, observer);
    }

    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, StateMachine stateMachine,
            int initTerm, Role initRole,
            RaftObserver observer) throws InterruptedException, SnapshotInstallException, FileNotFoundException, IOException, LogException {
        this.memberConnector = new RaftMemberConnector(memberConnector);
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
        this.votedFor = log.getVotedFor();
        this.channelPipelineInitializer = messageReceiver.getChannelPipelineInitializer();
        this.snapshotSender = new SnapshotSender(this.channelPipelineInitializer,
                new MemberId(messageReceiver.getMemberId().getHostName(),
                        configuration.getSnapshotSenderPort()),
                x -> {
                    snapshotSenderEventHandler(x);
                });
        initializeFromTheLatestSnapshot();
        memberConnectorObserver = new MemberConnectorObserverImpl(this, this.channelPipelineInitializer);
        configuration.getMemberIds().forEach(memberId -> memberConnector.register(memberId, memberConnectorObserver));
    }

    private void initializeFromTheLatestSnapshot()
            throws InterruptedException, SnapshotInstallException, FileNotFoundException, IOException, LogException {
        SnapshotDescriptor latestSnapshot = SnapshotDescriptor.getLatestSnapshotDescriptor(configuration.getSnapshotFolderPath());
        if (latestSnapshot != null) {
            log.installSnapshot(stateMachine, latestSnapshot);
            this.snapshotSender.setSnapshotDescriptor(latestSnapshot);
        }
    }

    private void cancelAllSnapshotTransfers() {
        snapshotRecipients.forEach((memberId, channel) -> {
            logger.info("MemberId: {}. Aborting snapshot transfer.");
            channel.disconnect();
        });
        snapshotRecipients.clear();
    }

    private void snapshotSenderEventHandler(SnapshotTransferEvent x) {
        if (x instanceof SnapshotTransferConnectionOpenedEvent) {
            SnapshotTransferConnectionOpenedEvent event = (SnapshotTransferConnectionOpenedEvent) x;
            logger.info("MemberId: {} connected to the snapshot sender. SnapshotDescriptor: {}.", event.getMemberId(), event.getSnapshotDescriptor());
            Channel prevValue = snapshotRecipients.put(event.getMemberId(), event.getChannel());
            if (prevValue != null) {
                prevValue.close();
            }
        } else if (x instanceof SnapshotTransferStartedEvent) {
            SnapshotTransferStartedEvent event = (SnapshotTransferStartedEvent) x;
            logger.info("MemberId: {}. SnapshotDescriptor: {}. Snapshot transfer started.", event.getMemberId(), event.getSnapshotDescriptor());
        } else if (x instanceof SnapshotTransferCompletedEvent) {
            SnapshotTransferCompletedEvent event = (SnapshotTransferCompletedEvent) x;
            logger.info("MemberId: {}. SnapshotDescriptor: {}. Snapshot transfer completed.", event.getMemberId(), event.getSnapshotDescriptor());
            snapshotRecipients.remove(event.getMemberId());
        } else if (x instanceof SnapshotTransferConnectionClosedEvent) {
            SnapshotTransferConnectionClosedEvent event = (SnapshotTransferConnectionClosedEvent) x;
            logger.info("MemberId: {}. SnapshotDescriptor: {}. Closing.", event.getMemberId(), event.getSnapshotDescriptor());
            snapshotRecipients.remove(event.getMemberId());
        } else if (x instanceof SnapshotTransferExceptionThrownEvent) {
            SnapshotTransferExceptionThrownEvent event = (SnapshotTransferExceptionThrownEvent) x;
            logger.info("MemberId: {}. SnapshotDescriptor: {}. Error occured.", event.getMemberId(), event.getSnapshotDescriptor(), event.getThrowable());
            Channel prevValue = snapshotRecipients.remove(event.getMemberId());
            if (prevValue != null) {
                prevValue.close();
            }
        }
    }

    private void initializeEventLoop() {
        EventLoopGroup prevEventLoop = eventLoop.getAndSet(
                configuration.getMainEventLoopThreadPoolSize() > 0 ? new NioEventLoopGroup(configuration.getMainEventLoopThreadPoolSize()) : new NioEventLoopGroup());

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
            if (isVotingMember()) {
                scheduleHeartbeatMonitorTask();
            }
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
            cancelAllSnapshotTransfers();
            messageReceiver.shutdown();
            snapshotSender.shutdown();
            eventLoop.get().shutdownGracefully();
            started = false;
            observer.shutdown();
        }
    }

    @Override
    public void consumeRequestVote(Member member, RequestVote requestVote) {
        boolean ignoreMessage = checkTermRecency(requestVote.getTerm());
        if (ignoreMessage || isCatchingUpMember()) {
            return;
        }
        Builder response = RequestVoteResponse.newBuilder();

        boolean grantVote = false;

        synchronized (stateLock) {
            response.setTerm(getCurrentTerm());
            MemberId candidateId = MemberId.fromString(requestVote.getCandidateId());
            if (votedFor == null || votedFor.equals(candidateId)) {
                try {
                    grantVote = checkCandidatesLogIsUpToDate(requestVote);
                } catch (LogException e) {
                    logger.error(severe, "Member: {}. Term: {}. checkCandidatesLogIsUpToDate failed.", member.getMemberId(), getCurrentTerm(), e);
                }
            }

            if (grantVote) {
                votedFor = candidateId;
                log.setVotedFor(votedFor);
                logger.debug("Member: {}. Term: {}. Giving vote to: {}.", selfId, getCurrentTerm(), votedFor);
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
                logger.debug("Member: {}. Term: {}. Votes received: {}.", selfId, getCurrentTerm(), votesReceived);
            } else {
                observer.voteRejected();
            }

            if (votesReceived >= getMajority()) {
                becomeLeader();
            }
        }
    }

    private boolean checkCandidatesLogIsUpToDate(RequestVote requestVote) throws LogException {
        if (log.getLast().getTerm() == requestVote.getLastLogTerm()) {
            return requestVote.getLastLogIndex() >= log.getLastIndex();
        }
        return requestVote.getLastLogTerm() >= log.getLast().getTerm();
    }

    private int compareAndUpdateCurrentTerm(int term) {
        synchronized (stateLock) {
            if (term > currentTerm) {
                currentTerm = term;
                log.setTerm(currentTerm);
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
            logger.debug("Member: {}. Term: {}. Votes Received: {}. BECAME LEADER.", selfId, getCurrentTerm(), votesReceived);
            cancelElectionTimeoutTask();
            scheduleSendHeartbeats();
            schedulePeriodicHeartbeatTask();
            role = Role.Leader;
            leader = selfId;
            votesReceived = 0;

            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> {
                        updateMemberIdRelatedBookkeeping(memberId);
                    });
        }
        applyNoOperationCommand();
        observer.electionWon(getCurrentTerm(), this);
    }

    private void updateMemberIdRelatedBookkeeping(MemberId memberId) {
        nextIndexes.put(memberId, new AtomicLong(log.getLastIndex() + 1));
        matchIndexes.put(memberId, new AtomicLong(0));
        replicationRetryTasks.putIfAbsent(memberId, new AtomicReference<ScheduledFuture<?>>(null));
        scheduleAppendEntries(memberId);
    }

    private void removeMemberIdRelatedBookkeeping(MemberId memberId) {
        cancelAppendEntriesRetry(memberId);
        replicationRetryTasks.remove(memberId);

        indexesLock.writeLock().lock();
        try {
            nextIndexes.remove(memberId);
            matchIndexes.remove(memberId);
        } finally {
            indexesLock.writeLock().unlock();
        }
    }

    private void becomeFollower() {
        if (isStarted()) {
            cancelElectionTimeoutTask();
            cancelPeriodicHeartbeatTask();
            if (isVotingMember()) {
                scheduleHeartbeatMonitorTask();
            }
        }

        synchronized (stateLock) {
            role = Role.Follower;
            votedFor = null;
            log.setVotedFor(null);
            votesReceived = 0;
            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> {
                        replicationRetryTasks.putIfAbsent(memberId, new AtomicReference<ScheduledFuture<?>>(null));
                        cancelTask(replicationRetryTasks.get(memberId).getAndSet(null));
                    });

            memberConnector.unregisterAllCatchingUpServers();
            cancelAllSnapshotTransfers();

            replicationCompletableFutures.values().forEach(future -> future.complete(false));
            replicationCompletableFutures.clear();
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

        try {
            if (appendEntries.hasInstallSnapshot()) {
                processInstallSnapshot(appendEntries.getInstallSnapshot());
            } else if (appendEntries.getLogEntriesCount() == 0) {
                processHeartbeat(appendEntries);
                log.commit(appendEntries.getLeaderCommitIndex(), stateMachine);
            } else {
                boolean successFlag = processAppendEntries(appendEntries);
                long indexOfFirstNewEntry = appendEntries.getPrevLogIndex() + 1;
                long indexOfLastNewEntry = appendEntries.getPrevLogIndex() + appendEntries.getLogEntriesCount();
                if (successFlag) {
                    log.commit(Math.min(appendEntries.getLeaderCommitIndex(), indexOfLastNewEntry), stateMachine);
                } else {
                    log.commit(appendEntries.getLeaderCommitIndex(), stateMachine);
                }
                sendAppendEntriesResponse(member, getAppendEntriesResponse(indexOfFirstNewEntry, indexOfLastNewEntry, successFlag));
            }
            logCompaction();
        } catch (LogException e) {
            logger.error(severe, "Member: {}. Term: {}. consumeAppendEntries failed.", member.getMemberId(), getCurrentTerm(), e);
        }
    }

    private void logCompaction() throws LogException {
        if (configuration.getLogCompactionThreshold() == 0 ||
                log.getCommitIndex() - log.getFirstIndex() < configuration.getLogCompactionThreshold()) {
            return;
        }

        logCompactionTask.updateAndGet(compactionTask -> {
            if (!compactionTask.isDone()) {
                return compactionTask;
            }

            return CompletableFuture.runAsync(() -> {
                try {
                    snapshotSender.setSnapshotDescriptor(log.getSnapshotAndTruncate(stateMachine));
                } catch (Exception e) {
                    logger.error(severe, "{} Snapshot/Log Compaction failed.", log, e);
                }
            }, eventLoop.get());
        });
    }

    private void sendAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse) {
        RaftMessage responseMessage = RaftMessage.newBuilder()
                .setType(MessageType.AppendEntriesResponse)
                .setAppendEntriesResponse(appendEntriesResponse).build();
        member.getChannel().writeAndFlush(responseMessage);
    }

    private AppendEntriesResponse getAppendEntriesResponse(long indexOfFirstNewEntry, long indexOfLastNewEntry, boolean successFlag) {
        AppendEntriesResponse.Builder appendEntriesResponse = AppendEntriesResponse.newBuilder();
        return appendEntriesResponse
                .setSuccess(successFlag)
                .setTerm(getCurrentTerm())
                .setStartIndex(indexOfFirstNewEntry)
                .setEndIndex(indexOfLastNewEntry)
                .setLogLength(log.getLastIndex())
                .build();
    }

    private void processInstallSnapshot(SnapshotDownloadRequest request) {
        synchronized (snapshotInstallLock) {
            if (snapshotReceiver == null || snapshotReceiver.isDone()) {
                final SnapshotDescriptor snapshotDescriptor = new SnapshotDescriptor(configuration.getSnapshotFolderPath(), SnapshotMetadata.fromRequest(request));
                final MemberId snapshotSenderMemberId = MemberId.fromString(request.getMemberId());
                try {
                    snapshotReceiver = new SnapshotReceiver(channelPipelineInitializer,
                            selfId, snapshotSenderMemberId, snapshotDescriptor,
                            (SnapshotDescriptor sd, Throwable e) -> {
                                try {
                                    if (e != null) {
                                        throw new IllegalStateException(e);
                                    }
                                    log.installSnapshot(stateMachine, sd);
                                } catch (Exception e1) {
                                    logger.error(severe, "Member: {}. Snapshot: {}. Installation failed.",
                                            snapshotSenderMemberId,
                                            sd,
                                            e1);
                                }
                            });
                } catch (Exception e) {
                    logger.error(severe, "Member: {}. Snapshot: {}. Receiver initialization failed.",
                            snapshotSenderMemberId,
                            snapshotDescriptor,
                            e);
                }
            }
        }
    }

    private void processRaftCommands(List<LogEntry> logEntries) {
        List<LogEntry> raftCommands = logEntries.stream().filter(x -> x.getType() == LogEntryType.RaftProtocolCommand)
                .collect(Collectors.toList());

        for (LogEntry le : raftCommands) {
            try {
                DynamicMembershipChangeCommand command = DynamicMembershipChangeCommand.parseFrom(le.getEntry().toByteArray());
                MemberId memberId = MemberId.fromString(command.getMemberId());
                if (command.getType() == CommandType.AddMember) {
                    logger.info("Member: {}. Term: {}. Adding {} to the cluster.", selfId, getCurrentTerm(), memberId);
                    if (!memberId.equals(selfId)) {
                        memberConnector.register(memberId, memberConnectorObserver);
                        memberConnector.connect(memberId);
                    }
                } else {
                    memberConnector.unregister(memberId);
                    logger.info("Member: {}. Term: {}. Removing {} from from cluster.", selfId, getCurrentTerm(), memberId);
                }
            } catch (InvalidProtocolBufferException e) {
                logger.error("Error on processing raft command.", e);
            }
        }
    }

    private boolean processAppendEntries(AppendEntries appendEntries) throws LogException {
        List<LogEntry> logEntries = appendEntries.getLogEntriesList();
        processRaftCommands(logEntries);
        return log.append(appendEntries.getPrevLogIndex(), appendEntries.getPrevLogTerm(), logEntries);
    }

    @Override
    public void consumeAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse) {
        boolean ignoreMessage = checkTermRecency(appendEntriesResponse.getTerm());
        if (ignoreMessage) {
            return;
        }

        if (!isLeader()) {
            return;
        }

        boolean isAccepted = appendEntriesResponse.getSuccess();
        if (isAccepted) {
            processSuccessfulAppendEntriesResponse(member, appendEntriesResponse);
        } else {
            processFailedAppendEntriesResponse(member, appendEntriesResponse);
        }
    }

    private void processSuccessfulAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse) {
        indexesLock.writeLock().lock();
        try {
            AtomicLong nextIndexRef = nextIndexes.get(member.getMemberId());
            AtomicLong matchIndexRef = matchIndexes.get(member.getMemberId());

            if (nextIndexRef.get() != appendEntriesResponse.getStartIndex()) {
                return;
            }

            long newMatchIndex = appendEntriesResponse.getEndIndex();
            long newNextIndex = appendEntriesResponse.getEndIndex() + 1;

            nextIndexRef.set(newNextIndex);
            matchIndexRef.set(newMatchIndex);

            if (newNextIndex <= log.getLastIndex()) {
                scheduleAppendEntries(member.getMemberId());
            } else {
                cancelAppendEntriesRetry(member.getMemberId());
            }
        } finally {
            indexesLock.writeLock().unlock();
        }

        try {
            checkReplicationStatus();
        } catch (LogException e) {
            logger.error(severe, "Member: {}. Term: {}. Replication check failed due to log exception.", member.getMemberId(), getCurrentTerm(), e);
        }
    }

    private void processFailedAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse) {
        indexesLock.writeLock().lock();
        try {
            AtomicLong nextIndexRef = nextIndexes.get(member.getMemberId());
            if (nextIndexRef.get() == appendEntriesResponse.getStartIndex()) {
                nextIndexRef.set(appendEntriesResponse.getLogLength() + 1);
            }
        } finally {
            indexesLock.writeLock().unlock();
        }
        scheduleAppendEntries(member.getMemberId());
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
        cancelTask(newElectionInitiatorTask.getAndSet(getNextElectionTask()));
    }

    private ScheduledFuture<?> getNextElectionTask() {
        observer.nextElectionScheduled();
        final int timeout = random.nextInt(configuration.getElectionMaxTimeout() - configuration.getElectionMinTimeout()) + configuration.getElectionMinTimeout();
        return eventLoop.get().schedule(() -> RaftImpl.this.heartbeatTimedout(), timeout, TimeUnit.MILLISECONDS);
    }

    private void scheduleElectionTimeoutTask() {
        final int timeout = random.nextInt(configuration.getElectionMaxTimeout() - configuration.getElectionMinTimeout()) + configuration.getElectionMinTimeout();
        ScheduledFuture<?> prevTask = electionTimeoutMonitorTask.getAndSet(
                eventLoop.get().schedule(() -> RaftImpl.this.electionTimedout(), timeout, TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private void schedulePeriodicHeartbeatTask() {
        ScheduledFuture<?> prevTask = periodicHeartbeatTask.getAndSet(
                eventLoop.get().scheduleAtFixedRate(() -> RaftImpl.this.scheduleSendHeartbeats(), 0, configuration.getHeartbeatInterval(), TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private ApplyCommandResult applyNoOperationCommand() {
        return applyCommand(LogEntryType.NoOperationCommand, new byte[0]);
    }

    private long logAppend(LogEntry logEntry) {
        if (logEntry.getType() == LogEntryType.RaftProtocolCommand) {
            List<LogEntry> raftCommand = new ArrayList<>();
            raftCommand.add(logEntry);
            processRaftCommands(raftCommand);
        }
        return log.append(logEntry);
    }

    private ApplyCommandResult applyCommand(LogEntryType type, byte[] command) {
        if (!isLeader()) {
            return new ApplyCommandResult(null, getLeaderMemberId());
        }

        LogEntry logEntry = LogEntryFactory.create(getCurrentTerm(), type, command);
        ApplyCommandFuture applyCommandFuture = scheduleReplication(logAppend(logEntry));

        synchronized (stateLock) {
            return new ApplyCommandResult(applyCommandFuture, getLeaderMemberId());
        }
    }

    @Override
    public ApplyCommandResult applyCommand(byte[] command) {
        return applyCommand(LogEntryType.StateMachineCommand, command);
    }

    private ApplyCommandFuture scheduleReplication(long index) {
        ApplyCommandFuture applyCommandFuture = new ApplyCommandFuture();
        replicationCompletableFutures.put(index, applyCommandFuture);

        for (MemberId memberId : memberConnector.getRegisteredMemberIds()) {
            long nextIndex = 0;
            indexesLock.readLock().lock();
            try {
                AtomicLong nextIndexInt = nextIndexes.get(memberId);
                if (nextIndexInt == null) {
                    throw new IllegalStateException("nextIndex is not initialized");
                }
                nextIndex = nextIndexInt.get();
            } finally {
                indexesLock.readLock().unlock();
            }
            if (nextIndex <= log.getLastIndex()) {
                scheduleAppendEntries(memberId);
            }
        }

        return applyCommandFuture;
    }

    private void cancelAppendEntriesRetry(MemberId memberId) {
        cancelTask(replicationRetryTasks.get(memberId).get());
    }

    private void scheduleAppendEntriesRetry(MemberId memberId) {
        try {
            observer.appendEntriesRetryScheduled(memberId);
            ScheduledFuture<?> future = eventLoop.get().schedule(() -> this.scheduleAppendEntries(memberId), configuration.getReplicationRetryInterval(), TimeUnit.MILLISECONDS);
            cancelTask(replicationRetryTasks.get(memberId).getAndSet(future));
        } catch (RejectedExecutionException e) {
            logger.error(severe, "Member: {}. Term: {}. Scheduling replication retry failed.", memberId, getCurrentTerm(), e);
        }
    }

    private void scheduleAppendEntries(MemberId memberId) {
        scheduleAppendEntriesRetry(memberId);
        Member member = memberConnector.getActiveMember(memberId);
        if (member != null) {
            try {
                scheduleSendMessageToMember(member, getAppendEntries(memberId));
            } catch (LogException e) {
                logger.error(severe, "Member: {}. Term: {}. Could not schedule replication.", memberId, getCurrentTerm(), e);
            }
        }
    }

    private void checkReplicationStatus() throws LogException {
        Iterator<Entry<Long, ApplyCommandFuture>> it = replicationCompletableFutures.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, ApplyCommandFuture> entry = it.next();
            long currIndex = entry.getKey();

            int replicationCount = 0;
            for (AtomicLong matchIndex : matchIndexes.values()) {
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
                if (log.get(currIndex).getTerm() == getCurrentTerm()) {
                    log.commit(entry.getKey(), stateMachine);
                    logCompaction();
                }
                entry.getValue().complete(true);
                it.remove();
            }
        }
    }

    private int getMajority() {
        return (memberConnector.getVotingMembersCount() + 1) / 2 + 1;
    }

    private void electionTimedout() {
        observer.electionTimedout();
        logger.debug("Member: {}. Term: {}. Election timedout.", selfId, getCurrentTerm());
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
            log.setTerm(currentTerm);
            votedFor = null;
            log.setVotedFor(votedFor);
            leader = null;
            votesReceived = 0;
            logger.debug("Member: {}. Term: {}. New election.", selfId, getCurrentTerm());
        }

        synchronized (stateLock) {
            if (votedFor == null) {
                ++votesReceived;
                votedFor = selfId;
                logger.debug("Member: {}. Term: {}. Votes Received: {}. Voted for itself.", selfId, getCurrentTerm(), votesReceived);
            } else {
                return;
            }
        }

        scheduleSendVoteRequests();
    }

    private void scheduleSendMessageToEachMember(RaftMessage msg) {
        memberConnector.getAllActiveMembers().forEach(member -> scheduleSendMessageToMember(member, msg));
    }

    private void scheduleSendMessageToEachVotingMember(RaftMessage msg) {
        memberConnector.getAllActiveVotingMembers().forEach(member -> scheduleSendMessageToMember(member, msg));
    }

    private ScheduledFuture<?> scheduleSendMessageToMember(Member member, RaftMessage msg) {
        try {
            return member.getChannel().eventLoop().schedule(() -> RaftImpl.this.sendMessage(member, msg), 0, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.error("Member: {}. Term: {}. Message type: {}. Message sending failed.", member, getCurrentTerm(), msg.getType(), e);
        }
        return null;
    }

    private void scheduleSendVoteRequests() {
        try {
            scheduleSendMessageToEachVotingMember(getRequestVoteMessage());
        } catch (LogException e) {
            logger.error(severe, "Term: {}. scheduleSendVoteRequests failed.", getCurrentTerm(), e);
        }
    }

    private void scheduleSendHeartbeats() {
        scheduleSendMessageToEachMember(getHeartbeatMessage());
    }

    private RaftMessage getAppendEntries(MemberId memberId) throws LogException {
        long nextIndex = 0;
        indexesLock.readLock().lock();
        try {
            nextIndex = nextIndexes.get(memberId).get();
        } finally {
            indexesLock.readLock().unlock();
        }

        AppendEntries.Builder req = AppendEntries.newBuilder();
        MemberId leaderMemberId = getLeaderMemberId();

        if (log.getFirstIndex() > nextIndex) {
            SnapshotDescriptor snapshot = snapshotSender.getSnapshotDescriptor();
            req.setTerm(getCurrentTerm())
                    .setLeaderId(leaderMemberId != null ? leaderMemberId.toString() : null)
                    .setInstallSnapshot(SnapshotDownloadRequest.newBuilder()
                            .setSnapshotId(snapshot.getMetadata().getSnapshotId())
                            .setSize(snapshot.getMetadata().getSize())
                            .setMemberId(snapshotSender.getMemberId().toString())
                            .setTerm(snapshot.getMetadata().getTerm())
                            .setIndex(snapshot.getMetadata().getIndex())
                            .addAllMembers(snapshot.getMetadata().getMembers().stream().map(x -> x.toString()).collect(Collectors.toSet())));
        } else {
            List<LogEntry> logEntries = log.get(nextIndex, configuration.getMaxNumberOfLogEntriesPerRequest());
            if (logEntries.size() == 0) {
                return getHeartbeatMessage();
            }

            long prevLogIndex = nextIndex - 1;
            int prevLogTerm = log.get(prevLogIndex).getTerm();
            long commitIndex = log.getCommitIndex();

            req.setTerm(getCurrentTerm())
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .setLeaderCommitIndex(commitIndex)
                    .setLeaderId(leaderMemberId != null ? leaderMemberId.toString() : null);

            req.addAllLogEntries(logEntries);
        }

        RaftMessage requestVote = RaftMessage.newBuilder()
                .setType(MessageType.AppendEntries)
                .setAppendEntries(req)
                .build();

        return requestVote;
    }

    private RaftMessage getRequestVoteMessage() throws LogException {
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
                .setAppendEntries(req.setLeaderCommitIndex(log.getCommitIndex()))
                .build();

        return requestVote;
    }

    private void sendMessage(Member member, RaftMessage req) {
        Channel memberChannel = member.getChannel();
        if (memberChannel.isActive()) {
            if (req.getType() == RaftMessage.MessageType.RequestVote) {
                logger.debug("Member: {}. Term: {}. Vote request sent to: {}.", selfId, getCurrentTerm(), member.getMemberId());
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

    private boolean isLeader() {
        return getRole() == Role.Leader;
    }

    @Override
    public Log getLog() {
        return log;
    }

    @Override
    public AddCatchingUpMemberResult addCatchingUpMember(MemberId memberId) {
        if (!isLeader()) {
            return new AddCatchingUpMemberResult(false, getLeaderMemberId());
        }

        if (memberConnector.getRegisteredMemberIds().contains(memberId)) {
            throw new IllegalArgumentException("Member has already been added");
        }

        memberConnector.registerAsCatchingUpMember(memberId, memberConnectorObserver);
        memberConnector.connect(memberId);

        updateMemberIdRelatedBookkeeping(memberId);

        return new AddCatchingUpMemberResult(true, getLeaderMemberId());
    }

    @Override
    public RemoveCatchingUpMemberResult removeCatchingUpMember(MemberId memberId) {
        if (!memberConnector.isCatchingUpMember(memberId)) {
            throw new IllegalArgumentException("Unknown member");
        }

        memberConnector.unregister(memberId);
        removeMemberIdRelatedBookkeeping(memberId);

        return new RemoveCatchingUpMemberResult(true, getLeaderMemberId());
    }

    private ApplyCommandResult addRemoveMemberDynamically(CommandType type, MemberId memberId) {
        return applyCommand(LogEntryType.RaftProtocolCommand,
                DynamicMembershipChangeCommand.newBuilder()
                        .setType(type)
                        .setMemberId(memberId.toString())
                        .build().toByteArray());
    }

    @Override
    public ApplyCommandResult addMemberDynamically(MemberId memberId) {
        if (!isLeader()) {
            return new ApplyCommandResult(null, getLeaderMemberId());
        }

        if (!memberConnector.getRegisteredMemberIds().contains(memberId)) {
            throw new IllegalArgumentException("Member must be added as a catching up member first.");
        }

        synchronized (dynamicMembershipChangeLock) {
            if (dynamicMembershipChangeInProgress != null &&
                    dynamicMembershipChangeInProgress.getResult() != null &&
                    !dynamicMembershipChangeInProgress.getResult().isDone()) {
                throw new IllegalStateException("Member adding is in progress.");
            }

            dynamicMembershipChangeInProgress = addRemoveMemberDynamically(CommandType.AddMember, memberId);
            return dynamicMembershipChangeInProgress;
        }
    }

    @Override
    public ApplyCommandResult removeMemberDynamically(MemberId memberId) {
        if (!isLeader()) {
            return new ApplyCommandResult(null, getLeaderMemberId());
        }

        if (selfId.equals(memberId) && isLeader()) {
            throw new IllegalArgumentException("Can't remove leader. Please initiate leader stepdown first.");
        }

        synchronized (dynamicMembershipChangeLock) {
            if (dynamicMembershipChangeInProgress != null &&
                    dynamicMembershipChangeInProgress.getResult() != null &&
                    !dynamicMembershipChangeInProgress.getResult().isDone()) {
                throw new IllegalStateException("Member removing is in progress.");
            }

            dynamicMembershipChangeInProgress = addRemoveMemberDynamically(CommandType.RemoveMember, memberId);
            return dynamicMembershipChangeInProgress;
        }
    }

    @Override
    public boolean isVotingMember() {
        return !catchingUpMember.get();
    }

    @Override
    public boolean isCatchingUpMember() {
        return catchingUpMember.get();
    }

    @Override
    public void becomeCatchingUpMember() {
        catchingUpMember.set(true);
        cancelHeartbeatMonitorTask();
        becomeFollower();
    }

    @Override
    public void becomeVotingMember() {
        catchingUpMember.set(false);
        scheduleHeartbeatMonitorTask();
    }

    public MemberId getLeaderMemberId() {
        synchronized (stateLock) {
            return leader;
        }
    }

}

package com.rvprg.sumi.protocol;

import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.log.Log;
import com.rvprg.sumi.log.LogEntryFactory;
import com.rvprg.sumi.log.LogException;
import com.rvprg.sumi.log.SnapshotInstallException;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.*;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.DynamicMembershipChangeCommand.CommandType;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RequestVoteResponse.Builder;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.transport.*;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.net.InetSocketAddress;
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

public class ConsensusImpl implements Consensus {
    private final Logger logger = LoggerFactory.getLogger(ConsensusImpl.class);
    private final static Marker severe = MarkerFactory.getMarker("SEVERE");

    private final MemberId selfId;
    private final Configuration configuration;
    private final ConsensusMemberConnector memberConnector;
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

    private final ConsensusEventListener listener;

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
    private MemberRole role = MemberRole.Follower;
    @GuardedBy("stateLock")
    private boolean started = false;
    private AtomicReference<EventLoopGroup> eventLoop = new AtomicReference<EventLoopGroup>(null);

    private final MemberConnectorListenerImpl memberConnectorListener;

    private final Random random;

    private final StateMachine stateMachine;

    private final AtomicBoolean catchingUpMember = new AtomicBoolean(false);
    private final ChannelPipelineInitializer channelPipelineInitializer;

    @Inject
    public ConsensusImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver,
                         Log log, StateMachine stateMachine, ConsensusEventListener listener)
            throws InterruptedException, SnapshotInstallException, LogException {
        this(configuration, memberConnector, messageReceiver, log, stateMachine, log.getTerm(), MemberRole.Follower, listener);
    }

    public ConsensusImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, Log log, StateMachine stateMachine,
                         int initTerm, MemberRole initRole,
                         ConsensusEventListener listener) throws InterruptedException, SnapshotInstallException, LogException {
        this.messageReceiver = messageReceiver;
        this.selfId = messageReceiver.getMemberId();
        this.log = log;
        this.currentTerm = initTerm;
        this.role = initRole;
        this.leader = null;
        this.configuration = configuration;
        this.random = new Random();
        this.listener = listener == null ? ConsensusEventListener.getDefaultInstance() : listener;
        this.stateMachine = stateMachine;
        this.votedFor = log.getVotedFor();
        this.channelPipelineInitializer = messageReceiver.getChannelPipelineInitializer();
        this.snapshotSender = new SnapshotSender(this.channelPipelineInitializer,
                new MemberId(messageReceiver.getMemberId().getHostName(),
                        configuration.getSnapshotSenderPort()),
                x -> {
                    snapshotSenderEventHandler(x);
                });
        this.memberConnectorListener = new MemberConnectorListenerImpl(this, this.channelPipelineInitializer);
        this.memberConnector = new ConsensusMemberConnector(memberConnector, memberConnectorListener);
        configuration.getMemberIds().forEach(memberId -> memberConnector.register(memberId, memberConnectorListener));
        initializeFromTheLatestSnapshot();
    }

    private void initializeFromTheLatestSnapshot()
            throws SnapshotInstallException, LogException {
        SnapshotDescriptor latestSnapshot = SnapshotDescriptor.getLatestSnapshotDescriptor(configuration.getSnapshotFolderPath());
        if (latestSnapshot != null) {
            logger.info("[{}] Initializing from the snapshot: {}.", getCurrentTerm(), latestSnapshot);
            log.installSnapshot(stateMachine, memberConnector, latestSnapshot);
            this.snapshotSender.setSnapshotDescriptor(latestSnapshot);
        }
    }

    private void cancelAllSnapshotTransfers() {
        snapshotRecipients.forEach((memberId, channel) -> {
            logger.info("[{}] MemberId: {}. Aborting snapshot transfer.", getCurrentTerm(), memberId);
            channel.disconnect();
        });
        snapshotRecipients.clear();
    }

    private void snapshotSenderEventHandler(SnapshotTransferEvent x) {
        if (x instanceof SnapshotTransferConnectionOpenedEvent) {
            SnapshotTransferConnectionOpenedEvent event = (SnapshotTransferConnectionOpenedEvent) x;
            logger.info("[{}] MemberId: {} connected to the snapshot sender. Snapshot: {}.", getCurrentTerm(), event.getMemberId(), event.getSnapshotDescriptor());
            Channel prevValue = snapshotRecipients.put(event.getMemberId(), event.getChannel());
            if (prevValue != null) {
                prevValue.close();
            }
        } else if (x instanceof SnapshotTransferStartedEvent) {
            SnapshotTransferStartedEvent event = (SnapshotTransferStartedEvent) x;
            logger.info("[{}] MemberId: {}. Snapshot: {}. Snapshot transfer started.", getCurrentTerm(), event.getMemberId(), event.getSnapshotDescriptor());
        } else if (x instanceof SnapshotTransferCompletedEvent) {
            SnapshotTransferCompletedEvent event = (SnapshotTransferCompletedEvent) x;
            logger.info("[{}] MemberId: {}. Snapshot: {}. Snapshot transfer completed.", getCurrentTerm(), event.getMemberId(), event.getSnapshotDescriptor());
            snapshotRecipients.remove(event.getMemberId());
        } else if (x instanceof SnapshotTransferConnectionClosedEvent) {
            SnapshotTransferConnectionClosedEvent event = (SnapshotTransferConnectionClosedEvent) x;
            logger.info("[{}] MemberId: {}. Snapshot: {}. Closing.", getCurrentTerm(), event.getMemberId(), event.getSnapshotDescriptor());
            snapshotRecipients.remove(event.getMemberId());
        } else if (x instanceof SnapshotTransferExceptionThrownEvent) {
            SnapshotTransferExceptionThrownEvent event = (SnapshotTransferExceptionThrownEvent) x;
            logger.info("[{}] MemberId: {}. Snapshot: {}. Error occured.", getCurrentTerm(), event.getMemberId(), event.getSnapshotDescriptor(), event.getThrowable());
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
            snapshotSender.start();
            messageReceiver.start(this);
            memberConnector.connectAllRegistered();
            if (isVotingMember()) {
                scheduleHeartbeatMonitorTask();
            }
            started = true;
            listener.started();
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
            listener.shutdown();
        }
    }

    @Override
    public void consumeRequestVote(ActiveMember member, RequestVote requestVote) {
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
                    logger.error(severe, "[{}] Member: {}. checkCandidatesLogIsUpToDate failed.", getCurrentTerm(), member.getMemberId(), e);
                }
            }

            if (grantVote) {
                votedFor = candidateId;
                log.setVotedFor(votedFor);
                logger.debug("[{}] Member: {}. Giving vote to: {}.", getCurrentTerm(), selfId, votedFor);
            }
        }

        RaftMessage responseMessage = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(grantVote).build())
                .build();
        member.getChannel().writeAndFlush(responseMessage);
    }

    @Override
    public void consumeRequestVoteResponse(ActiveMember member, RequestVoteResponse requestVoteResponse) {
        boolean ignoreMessage = checkTermRecency(requestVoteResponse.getTerm());
        if (ignoreMessage) {
            listener.voteRejected();
            return;
        }

        if (getRole() != MemberRole.Candidate) {
            return;
        }

        synchronized (stateLock) {
            boolean sameTerm = requestVoteResponse.getTerm() == getCurrentTerm();

            if (sameTerm && requestVoteResponse.getVoteGranted()) {
                listener.voteReceived();
                ++votesReceived;
                logger.debug("[{}] MemberId: {}. Votes received: {}.", getCurrentTerm(), selfId, votesReceived);
            } else {
                listener.voteRejected();
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
            if (getRole() != MemberRole.Candidate) {
                return;
            }
            logger.debug("[{}] MemberId: {}. Votes Received: {}. BECAME LEADER.", getCurrentTerm(), selfId, votesReceived);
            cancelElectionTimeoutTask();
            scheduleSendHeartbeats();
            schedulePeriodicHeartbeatTask();
            role = MemberRole.Leader;
            leader = selfId;
            votesReceived = 0;

            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> {
                        updateMemberIdRelatedBookkeeping(memberId);
                    });
        }
        applyNoOperationCommand();
        listener.electionWon(getCurrentTerm(), this);
    }

    private void updateMemberIdRelatedBookkeeping(MemberId memberId) {
        nextIndexes.put(memberId, new AtomicLong(log.getLastIndex() + 1));
        matchIndexes.put(memberId, new AtomicLong(0));
        replicationRetryTasks.putIfAbsent(memberId, new AtomicReference<>(null));
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
            role = MemberRole.Follower;
            votedFor = null;
            log.setVotedFor(null);
            votesReceived = 0;
            memberConnector.getRegisteredMemberIds().forEach(
                    memberId -> {
                        replicationRetryTasks.putIfAbsent(memberId, new AtomicReference<>(null));
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
    public void consumeAppendEntries(ActiveMember member, AppendEntries appendEntries) {
        boolean ignoreMessage = checkTermRecency(appendEntries.getTerm());
        if (ignoreMessage) {
            return;
        }

        synchronized (stateLock) {
            if (appendEntries.hasLeaderId()) {
                MemberId otherLeader = MemberId.fromString(appendEntries.getLeaderId());
                if (leader != null &&
                        !leader.equals(otherLeader) &&
                        role != MemberRole.Follower) {
                    becomeFollower();
                }
                leader = otherLeader;
            }
        }

        try {
            if (appendEntries.hasInstallSnapshot()) {
                logger.info("[{}] MemberId: {} Install snapshot request received", getCurrentTerm(), selfId);
                processInstallSnapshot(member, appendEntries.getInstallSnapshot());
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
            logger.error(severe, "[{}] MemberId: {}. consumeAppendEntries failed.", getCurrentTerm(), member.getMemberId(), e);
        }
    }

    private void logCompaction() throws LogException {
        if (configuration.getLogCompactionThreshold() == 0 ||
                log.getCommitIndex() - log.getFirstIndex() < configuration.getLogCompactionThreshold()) {
            return;
        }
        scheduleLogCompactionTask();
    }

    private void scheduleLogCompactionTask() {
        logCompactionTask.updateAndGet(compactionTask -> {
            if (!compactionTask.isDone()) {
                return compactionTask;
            }

            return CompletableFuture.runAsync(() -> {
                try {
                    snapshotSender.setSnapshotDescriptor(log.getSnapshotAndTruncate(stateMachine, memberConnector));
                } catch (Exception e) {
                    logger.error(severe, "[{}] {} Snapshot/Log Compaction failed.", getCurrentTerm(), log, e);
                }
            }, eventLoop.get());
        });
    }

    private void sendAppendEntriesResponse(ActiveMember member, AppendEntriesResponse appendEntriesResponse) {
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

    private AppendEntriesResponse getAppendEntriesFromSnapshotResponse(long index) {
        AppendEntriesResponse.Builder appendEntriesResponse = AppendEntriesResponse.newBuilder();
        return appendEntriesResponse
                .setSuccess(true)
                .setTerm(getCurrentTerm())
                .setEndIndex(index)
                .setLogLength(log.getLastIndex())
                .setSnapshotInstalled(true)
                .build();
    }

    private void processInstallSnapshot(final ActiveMember member, SnapshotDownloadRequest request) {
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
                                        throw e;
                                    }
                                    log.installSnapshot(stateMachine, memberConnector, sd);
                                    logger.info("[{}] MemberId: {}. Snapshot: {}. Installation successfull.", getCurrentTerm(), selfId, sd);
                                    listener.snapshotInstalled(sd);
                                    sendAppendEntriesSnapshotInstalledResponse(member, getAppendEntriesFromSnapshotResponse(request.getIndex()));
                                } catch (Throwable e1) {
                                    logger.error(severe, "[{}] MemberId: {}. Snapshot: {}. Installation failed.", getCurrentTerm(), selfId, snapshotDescriptor, e1);
                                }
                            });
                } catch (Exception e) {
                    logger.error(severe, "[{}] MemberId: {}. Snapshot: {}. Receiver initialization failed.",
                            getCurrentTerm(),
                            selfId,
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
                    logger.info("[{}] MemberId: {}. Adding {} to the cluster.", getCurrentTerm(), selfId, memberId);
                    if (!memberId.equals(selfId)) {
                        memberConnector.register(memberId, memberConnectorListener);
                        memberConnector.connect(memberId);
                    }
                } else {
                    memberConnector.unregister(memberId);
                    logger.info("[{}] MemberId: {}. Removing {} from from cluster.", getCurrentTerm(), selfId, memberId);
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
    public void consumeAppendEntriesResponse(ActiveMember member, AppendEntriesResponse appendEntriesResponse) {
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

    private void processSuccessfulAppendEntriesResponse(ActiveMember member, AppendEntriesResponse appendEntriesResponse) {
        indexesLock.writeLock().lock();
        try {
            AtomicLong nextIndexRef = nextIndexes.get(member.getMemberId());
            AtomicLong matchIndexRef = matchIndexes.get(member.getMemberId());
            boolean fromSnapshot = appendEntriesResponse.hasSnapshotInstalled() && appendEntriesResponse.getSnapshotInstalled();

            if (!fromSnapshot && nextIndexRef.get() != appendEntriesResponse.getStartIndex()) {
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
            logger.error(severe, "[{}] MemberId: {}. Replication check failed due to log exception.", getCurrentTerm(), member.getMemberId(), e);
        }
    }

    private void processFailedAppendEntriesResponse(ActiveMember member, AppendEntriesResponse appendEntriesResponse) {
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
        listener.heartbeatReceived();
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
        listener.nextElectionScheduled();
        final int timeout = random.nextInt(configuration.getElectionMaxTimeout() - configuration.getElectionMinTimeout()) + configuration.getElectionMinTimeout();
        return eventLoop.get().schedule(() -> ConsensusImpl.this.heartbeatTimedout(), timeout, TimeUnit.MILLISECONDS);
    }

    private void scheduleElectionTimeoutTask() {
        final int timeout = random.nextInt(configuration.getElectionMaxTimeout() - configuration.getElectionMinTimeout()) + configuration.getElectionMinTimeout();
        ScheduledFuture<?> prevTask = electionTimeoutMonitorTask.getAndSet(
                eventLoop.get().schedule(() -> ConsensusImpl.this.electionTimedout(), timeout, TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private void schedulePeriodicHeartbeatTask() {
        ScheduledFuture<?> prevTask = periodicHeartbeatTask.getAndSet(
                eventLoop.get().scheduleAtFixedRate(() -> ConsensusImpl.this.scheduleSendHeartbeats(), 0, configuration.getHeartbeatInterval(), TimeUnit.MILLISECONDS));
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
            listener.appendEntriesRetryScheduled(memberId);
            ScheduledFuture<?> future = eventLoop.get().schedule(() -> this.scheduleAppendEntries(memberId), configuration.getReplicationRetryInterval(), TimeUnit.MILLISECONDS);
            cancelTask(replicationRetryTasks.get(memberId).getAndSet(future));
        } catch (RejectedExecutionException e) {
            logger.error(severe, "[{}] MemberId: {}. Scheduling replication retry failed.", getCurrentTerm(), memberId, e);
        }
    }

    private void scheduleAppendEntries(MemberId memberId) {
        scheduleAppendEntriesRetry(memberId);
        ActiveMember member = memberConnector.getActiveMember(memberId);
        if (member != null) {
            try {
                scheduleSendMessageToMember(member, getAppendEntries(memberId));
            } catch (LogException e) {
                logger.error(severe, "[{}] MemberId: {}. Could not schedule replication.", getCurrentTerm(), memberId, e);
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
        listener.electionTimedOut();
        logger.debug("[{}] MemberId: {}. Election timedout.", getCurrentTerm(), selfId);
        initiateElection();
    }

    private void heartbeatTimedout() {
        listener.heartbeatTimedOut();
        initiateElection();
    }

    private void initiateElection() {
        cancelHeartbeatMonitorTask();
        scheduleElectionTimeoutTask();

        synchronized (stateLock) {
            role = MemberRole.Candidate;
            ++currentTerm;
            log.setTerm(currentTerm);
            votedFor = null;
            log.setVotedFor(votedFor);
            leader = null;
            votesReceived = 0;
            logger.debug("[{}] MemberId: {}. New election.", getCurrentTerm(), selfId);
        }

        synchronized (stateLock) {
            if (votedFor == null) {
                ++votesReceived;
                votedFor = selfId;
                logger.debug("[{}] MemberId: {}. Votes Received: {}. Voted for itself.", getCurrentTerm(), selfId, votesReceived);
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

    private void sendAppendEntriesSnapshotInstalledResponse(ActiveMember member, AppendEntriesResponse appendEntriesResponse) {
        RaftMessage responseMessage = RaftMessage.newBuilder()
                .setType(MessageType.AppendEntriesResponse)
                .setAppendEntriesResponse(appendEntriesResponse).build();
        scheduleSendMessageToMember(member, responseMessage);
    }

    private ScheduledFuture<?> scheduleSendMessageToMember(ActiveMember member, RaftMessage msg) {
        try {
            return member.getChannel().eventLoop().schedule(() -> ConsensusImpl.this.sendMessage(member, msg), 0, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.error("[{}] MemberId: {}. Message type: {}. Message sending failed.", getCurrentTerm(), member, msg.getType(), e);
        }
        return null;
    }

    private void scheduleSendVoteRequests() {
        try {
            scheduleSendMessageToEachVotingMember(getRequestVoteMessage());
        } catch (LogException e) {
            logger.error(severe, "[{}] scheduleSendVoteRequests failed.", getCurrentTerm(), e);
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
                            .addAllMembers(snapshot.getMetadata().getMembers().stream().map(InetSocketAddress::toString).collect(Collectors.toSet())));
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

    private void sendMessage(ActiveMember member, RaftMessage req) {
        Channel memberChannel = member.getChannel();
        if (memberChannel.isActive()) {
            if (req.getType() == RaftMessage.MessageType.RequestVote) {
                logger.debug("[{}] MemberId: {}. Vote request sent to: {}.", getCurrentTerm(), selfId, member.getMemberId());
            }
            memberChannel.writeAndFlush(req);
        }
    }

    @Override
    public MemberRole getRole() {
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
        return getRole() == MemberRole.Leader;
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

        memberConnector.registerAsCatchingUpMember(memberId, memberConnectorListener);
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

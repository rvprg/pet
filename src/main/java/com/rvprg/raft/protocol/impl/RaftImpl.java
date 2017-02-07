package com.rvprg.raft.protocol.impl;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.inject.Inject;
import com.rvprg.raft.configuration.Configuration;
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
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MessageReceiver;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;

public class RaftImpl implements Raft {
    private final Object thisLock = new Object();

    private Role role = Role.Follower;

    private final Configuration configuration;

    private final MemberConnector memberConnector;
    private final MessageReceiver messageReceiver;

    private final long heartbeatTimeout;

    private final int electionMinTimeout = 150;
    private final int electionMaxTimeout = 300;

    private final EventLoopGroup eventLoop = new NioEventLoopGroup();

    private final AtomicReference<ScheduledFuture<?>> electionTrigger = new AtomicReference<ScheduledFuture<?>>();
    private final AtomicReference<ScheduledFuture<?>> electionTimeoutTrigger = new AtomicReference<ScheduledFuture<?>>();

    private final AtomicReference<String> votedFor = new AtomicReference<>(null);

    private final RaftObserver observer;

    private final AtomicInteger term = new AtomicInteger(0);
    private final AtomicInteger votesReceived = new AtomicInteger(0);

    private final String selfId;

    private final Random random = new Random();

    @Inject
    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, RaftObserver observer) {
        this.heartbeatTimeout = configuration.getHeartbeatTimeout();
        this.memberConnector = memberConnector;
        this.messageReceiver = messageReceiver;
        this.configuration = configuration;
        this.observer = observer;
        this.selfId = null; // FIXME: set
    }

    @Override
    public void start() throws InterruptedException {
        // FIXME: Check if started.
        messageReceiver.start(this);
        scheduleHeartbeatTask();
    }

    @Override
    public void shutdown() {
        cancelHeartbeatTask();
        cancelElectionTimeoutTask();
        this.messageReceiver.shutdown();
        this.eventLoop.shutdownGracefully().awaitUninterruptibly();
    }

    @Override
    public void consumeRequestVote(Channel senderChannel, RequestVote requestVote) {
        Builder response = RequestVoteResponse.newBuilder().setTerm(getCurrentTerm());
        String currVotedFor = votedFor.get();
        boolean grantVote = false;

        if (requestVote.getTerm() < getCurrentTerm()) {
            grantVote = false;
        } else if (currVotedFor == null || currVotedFor.equalsIgnoreCase(requestVote.getCandidateId())) {
            grantVote = checkCandidatesLogIsUpToDate(requestVote);
        }

        if (grantVote) {
            votedFor.set(requestVote.getCandidateId());
        }

        RaftMessage responseMessage = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(grantVote).build())
                .build();
        senderChannel.writeAndFlush(responseMessage);
    }

    private boolean checkCandidatesLogIsUpToDate(RequestVote requestVote) {
        // TODO: implement
        return false;
    }

    @Override
    public void consumeRequestVoteResponse(Channel senderChannel, RequestVoteResponse requestVoteResponse) {
        observer.voteReceived();

        if (requestVoteResponse.getTerm() == getCurrentTerm()) {
            votesReceived.incrementAndGet();
        }

        if (votesReceived.get() >= getMajority()) {
            cancelElectionTimeoutTask();
            changeRole(Role.Leader);
            observer.electionWon();
        }
    }

    @Override
    public void consumeAppendEntries(Channel senderChannel, AppendEntries appendEntries) {
        if (appendEntries.getLogEntriesCount() == 0) {
            processHeartbeat(appendEntries);
        }
    }

    @Override
    public void consumeAppendEntriesResponse(Channel senderChannel, AppendEntriesResponse appendEntriesResponse) {
    }

    private ScheduledFuture<?> scheduleNextElectionTask() {
        observer.nextElectionScheduled();
        return this.eventLoop.schedule(() -> RaftImpl.this.heartbeatTimedout(), heartbeatTimeout, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeatTask() {
        cancelTask(electionTrigger.get());
    }

    private void scheduleHeartbeatTask() {
        cancelTask(electionTrigger.getAndSet(scheduleNextElectionTask()));
    }

    private void processHeartbeat(AppendEntries appendEntries) {
        observer.heartbeatReceived();
        scheduleHeartbeatTask();
    }

    private void cancelTask(ScheduledFuture<?> task) {
        if (task != null) {
            task.cancel(true);
        }
    }

    private void cancelElectionTimeoutTask() {
        cancelTask(electionTimeoutTrigger.get());
    }

    private void createElectionTimeoutTask() {
        final int timeout = random.nextInt(electionMaxTimeout - electionMinTimeout) + electionMinTimeout;
        ScheduledFuture<?> prevTask = electionTimeoutTrigger.getAndSet(
                this.eventLoop.schedule(() -> RaftImpl.this.electionTimedout(), timeout, TimeUnit.MILLISECONDS));
        cancelTask(prevTask);
    }

    private int getMajority() {
        return memberConnector.getActiveMembers().getAll().size() / 2 + 1;
    }

    private void changeRole(Role newRole) {
        synchronized (thisLock) {
            role = newRole;
        }
    }

    private void electionTimedout() {
        observer.electionTimedout();
        initiateElection();
    }

    private void heartbeatTimedout() {
        observer.heartbeatTimedout();
        initiateElection();
    }

    private void initiateElection() {
        cancelHeartbeatTask();
        changeRole(Role.Candidate);
        term.incrementAndGet();
        votedFor.set(null);
        votesReceived.set(0);

        if (votedFor.compareAndSet(null, selfId)) {
            votesReceived.incrementAndGet();
        }

        sendoutVoteRequests();
        createElectionTimeoutTask();
    }

    private void sendoutVoteRequests() {
    }

    public Role getRole() {
        synchronized (thisLock) {
            return role;
        }
    }

    @Override
    public int getCurrentTerm() {
        return term.get();
    }

}

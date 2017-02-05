package com.rvprg.raft.protocol.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.inject.Inject;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
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

    private final RaftObserver observer;

    @Inject
    public RaftImpl(Configuration configuration, MemberConnector memberConnector, MessageReceiver messageReceiver, RaftObserver observer) {
        this.heartbeatTimeout = configuration.getHeartbeatTimeout();
        this.memberConnector = memberConnector;
        this.messageReceiver = messageReceiver;
        this.configuration = configuration;
        this.observer = observer;
    }

    @Override
    public void start() throws InterruptedException {
        // FIXME: Check if started.
        messageReceiver.start(this);
        electionTrigger.set(scheduleNextElection());
    }

    @Override
    public void shutdown() {
        cancelHeartbeatTask();
        this.messageReceiver.shutdown();
        this.eventLoop.shutdownGracefully().awaitUninterruptibly();
    }

    @Override
    public void consumeRequestVote(Channel senderChannel, RequestVote requestVoteMessage) {
    }

    @Override
    public void consumeRequestVoteResponse(Channel senderChannel, RequestVoteResponse requestVoteMessage) {
    }

    @Override
    public void consumeAppendEntries(Channel senderChannel, AppendEntries appendEntriesMessage) {
        if (appendEntriesMessage.getLogEntriesCount() == 0) {
            processHeartbeat(appendEntriesMessage);
        }
    }

    @Override
    public void consumeAppendEntriesResponse(Channel senderChannel, AppendEntriesResponse appendEntriesResponse) {
    }

    private ScheduledFuture<?> scheduleNextElection() {
        observer.nextElectionScheduled();
        return this.eventLoop.schedule(() -> RaftImpl.this.sendVoteRequests(), heartbeatTimeout, TimeUnit.MILLISECONDS);
    }

    private void processHeartbeat(AppendEntries appendEntriesMessage) {
        observer.heartbeatReceived();
        cancelHeartbeatTask();
    }

    private void cancelHeartbeatTask() {
        ScheduledFuture<?> prevScheduledElection = electionTrigger.getAndSet(scheduleNextElection());
        if (prevScheduledElection != null) {
            prevScheduledElection.cancel(true);
        }
    }

    private void sendVoteRequests() {
        synchronized (thisLock) {
            role = Role.Candidate;
        }
        observer.electionInitiated();
    }

    public Role getRole() {
        synchronized (thisLock) {
            return role;
        }
    }

}

package com.rvprg.sumi.tests;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.sumi.Module;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.log.*;
import com.rvprg.sumi.protocol.*;
import com.rvprg.sumi.protocol.messages.ProtocolMessages;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.*;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RequestVoteResponse.Builder;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.tests.helpers.NetworkUtils;
import com.rvprg.sumi.transport.*;
import io.netty.channel.Channel;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ConsensusTest {
    private AppendEntries getAppendEntriesInstance() {
        AppendEntries requestAppendEntries = ProtocolMessages.AppendEntries.newBuilder()
                .setTerm(1)
                .setLeaderId("localhost:1234")
                .setPrevLogIndex(0)
                .setLeaderCommitIndex(1)
                .build();
        return requestAppendEntries;
    }

    @Test
    public void checkHeartbeatTimeout() throws InterruptedException, SnapshotInstallException, LogException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftListener);

        final AtomicLong lastHeartbeatTime = new AtomicLong();
        final AtomicLong requestVotesInitiatedTime = new AtomicLong();

        // Start as a follower with term set to 0.
        assertEquals(MemberRole.Follower, raft.getRole());
        assertEquals(0, raft.getCurrentTerm());

        // Register when was the last heartbeat sent.
        doAnswer((Answer<Void>) invocation -> {
            lastHeartbeatTime.set(System.currentTimeMillis());
            return null;
        }).when(raftListener).heartbeatReceived();

        // Set a latch on a new election event.
        final CountDownLatch electionInitiatedLatch = new CountDownLatch(1);
        doAnswer((Answer<Void>) invocation -> {
            requestVotesInitiatedTime.set(System.currentTimeMillis());
            electionInitiatedLatch.countDown();
            return null;
        }).when(raftListener).heartbeatTimedOut();

        // This will wait until an absence of heartbeats is detected. This
        // event triggers a schedule of the next election. Therefore we
        // immediately cancel it two times by sending desired heartbeats,
        // so as to check that the action on receiving a heartbeat works as
        // expected.
        final CountDownLatch nextElectionScheduledLatch = new CountDownLatch(2);
        doAnswer((Answer<Void>) invocation -> {
            if (nextElectionScheduledLatch.getCount() > 0) {
                nextElectionScheduledLatch.countDown();
                raft.consumeAppendEntries(null, getAppendEntriesInstance());
            }
            return null;
        }).when(raftListener).nextElectionScheduled();

        raft.start();

        // This will cancel a new election twice, as two heartbeats will be
        // sent. Then it will initiate a new election.
        nextElectionScheduledLatch.await();
        // Wait until a new election is initiated.
        electionInitiatedLatch.await();

        raft.shutdown();

        assertTrue(raft.getCurrentTerm() > 0);

        verify(raftListener, atLeast(1)).heartbeatReceived();
        verify(raftListener, atLeast(1)).heartbeatTimedOut();
        assertTrue(requestVotesInitiatedTime.get() - lastHeartbeatTime.get() >= configuration.getElectionMinTimeout());
    }

    @Test
    public void testElectionTimeout() throws InterruptedException, LogException, SnapshotInstallException {
        int port = NetworkUtils.getRandomFreePort();
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", port)).snapshotSenderPort(NetworkUtils.getRandomFreePort())
                .logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MembersRegistry memberRegistry = mock(MembersRegistry.class);
        StateMachine stateMachine = mock(StateMachine.class);
        Set<ActiveMember> members = new HashSet<>();
        Mockito.when(memberRegistry.getAll()).thenReturn(members);
        Mockito.when(memberConnector.getActiveMembers()).thenReturn(memberRegistry);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", port));
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        LogEntry logEntry = LogEntryFactory.create(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.getLastIndex()).thenReturn(0L);

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftListener);
        // Start as a follower with term set to 0.
        assertEquals(MemberRole.Follower, raft.getRole());
        assertEquals(0, raft.getCurrentTerm());

        // This set up tests that election time out works. Once a new election
        // has been initiated, we allow it to time out twice. We should be able
        // to see that the term at least 2, and that we are still a Candidate.
        final CountDownLatch electionTimedoutLatch = new CountDownLatch(2);
        doAnswer((Answer<Void>) invocation -> {
            electionTimedoutLatch.countDown();
            return null;
        }).when(raftListener).electionTimedOut();

        raft.start();

        electionTimedoutLatch.await();

        raft.shutdown();

        // At least two terms further.
        assertTrue(raft.getCurrentTerm() >= 2);
        assertEquals(MemberRole.Candidate, raft.getRole());
    }

    @Test
    public void testConsumeVoteRequest_GiveVoteOnce() throws LogException, InterruptedException, SnapshotInstallException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        ActiveMember member = mock(ActiveMember.class);
        Channel senderChannel = mock(Channel.class);
        Mockito.when(member.getChannel()).thenReturn(senderChannel);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(configuration.getSelfId());

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftListener);

        assertEquals(0, raft.getCurrentTerm());
        assertEquals(0, log.getCommitIndex());

        RequestVote requestVote0 = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(0)
                .setCandidateId("localhost:1234")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        LogEntry logEntry = LogEntryFactory.create(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.getLastIndex()).thenReturn(0L);

        raft.consumeRequestVote(member, requestVote0);

        Builder response0 = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse0 = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response0.setVoteGranted(true).setTerm(0).build())
                .build();

        verify(senderChannel, times(1)).writeAndFlush(eq(expectedResponse0));

        // Different candidate.
        RequestVote requestVote1 = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(0)
                .setCandidateId("localhost:1235")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        raft.consumeRequestVote(member, requestVote1);

        Builder response1 = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse1 = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response1.setVoteGranted(false).setTerm(0).build())
                .build();

        verify(senderChannel, times(1)).writeAndFlush(eq(expectedResponse1));
    }

    @Test
    public void testConsumeVoteRequest_GiveVoteSameCandidate() throws LogException, InterruptedException, SnapshotInstallException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        ActiveMember member = mock(ActiveMember.class);
        Channel senderChannel = mock(Channel.class);
        Mockito.when(member.getChannel()).thenReturn(senderChannel);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftListener);

        assertEquals(0, raft.getCurrentTerm());
        assertEquals(0, log.getCommitIndex());

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(0)
                .setCandidateId("localhost:1234")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        LogEntry logEntry = LogEntryFactory.create(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.getLastIndex()).thenReturn(0L);

        raft.consumeRequestVote(member, requestVote);

        Builder response = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(true).setTerm(0).build())
                .build();

        verify(senderChannel, times(1)).writeAndFlush(eq(expectedResponse));

        // Send the same request
        raft.consumeRequestVote(member, requestVote);

        // Should grant vote.
        verify(senderChannel, times(2)).writeAndFlush(eq(expectedResponse));
    }

    @Test
    public void testConsumeVoteRequest_GiveVoteIfLogIsAsUpToDateAsReceivers()
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, LogException, InterruptedException,
            SnapshotInstallException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        ActiveMember member = mock(ActiveMember.class);
        Channel senderChannel = mock(Channel.class);
        Mockito.when(member.getChannel()).thenReturn(senderChannel);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftListener);

        Method initializeEventLoop = ConsensusImpl.class.getDeclaredMethod("initializeEventLoop", new Class[] {});
        initializeEventLoop.setAccessible(true);
        initializeEventLoop.invoke(raft, new Object[] {});

        assertEquals(0, raft.getCurrentTerm());
        assertEquals(0, log.getCommitIndex());

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(1)
                .setCandidateId("localhost:1234")
                .setLastLogIndex(1)
                .setLastLogTerm(1)
                .build();

        LogEntry logEntry = LogEntryFactory.create(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.getLastIndex()).thenReturn(0L);

        raft.consumeRequestVote(member, requestVote);

        Builder response = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(true).setTerm(1).build())
                .build();

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponse));

        // Assume receiver's log is more up to date then no vote should be
        // granted (i.e. last entry has more recent term, 1 != 2)
        logEntry = LogEntryFactory.create(2);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        // Mockito.when(logEntry.getTerm()).thenReturn(2);

        raft.consumeRequestVote(member, requestVote);

        Builder responseNoVoteGranted = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponseNoVoteGranted = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(responseNoVoteGranted.setVoteGranted(false).setTerm(1).build())
                .build();

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponseNoVoteGranted));

        // Assume receiver's log ends with the same term, but its length is
        // longer than candidates, so no vote should be granted.
        logEntry = LogEntryFactory.create(1);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.getLastIndex()).thenReturn(2L);

        raft.consumeRequestVote(member, requestVote);

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponseNoVoteGranted));
    }

    @Test
    public void testConsumeVoteRequest_DontGiveVoteIfTermIsOutdated() throws InterruptedException, SnapshotInstallException, LogException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        ActiveMember member = mock(ActiveMember.class);
        Channel senderChannel = mock(Channel.class);
        Mockito.when(member.getChannel()).thenReturn(senderChannel);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, 2, MemberRole.Follower, raftListener);

        assertEquals(2, raft.getCurrentTerm());

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(1)
                .setCandidateId("test")
                .setLastLogIndex(1)
                .setLastLogTerm(1)
                .build();

        raft.consumeRequestVote(member, requestVote);

        verify(senderChannel, never()).writeAndFlush(any());
    }

    @Test
    public void testConsumeVoteRequestResponse_CheckMajorityRule()
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException,
            SnapshotInstallException, LogException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        AtomicInteger votesReceived = new AtomicInteger(0);
        AtomicInteger votesRejected = new AtomicInteger(0);
        AtomicBoolean electionWon = new AtomicBoolean();
        ConsensusEventListener raftListener = new ConsensusEventListenerImpl() {

            @Override
            public void voteReceived() {
                votesReceived.incrementAndGet();
            }

            @Override
            public void electionWon(int term, Consensus leader) {
                electionWon.set(true);
            }

            @Override
            public void voteRejected() {
                votesRejected.incrementAndGet();
            }
        };

        Log log = new InMemoryLogImpl();
        ActiveMember member = mock(ActiveMember.class);
        Channel senderChannel = mock(Channel.class);
        Mockito.when(member.getChannel()).thenReturn(senderChannel);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, 1, MemberRole.Candidate, raftListener);

        Method initializeEventLoop = ConsensusImpl.class.getDeclaredMethod("initializeEventLoop", new Class[] {});
        initializeEventLoop.setAccessible(true);
        initializeEventLoop.invoke(raft, new Object[] {});

        assertEquals(1, raft.getCurrentTerm());

        RequestVoteResponse requestVoteResponseTerm1 = ProtocolMessages.RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .build();
        RequestVoteResponse requestVoteResponseTerm0 = ProtocolMessages.RequestVoteResponse.newBuilder()
                .setTerm(0)
                .setVoteGranted(true)
                .build();

        @SuppressWarnings("unchecked")
        Set<MemberId> membersSet = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            membersSet.add(new MemberId("localhost", i));
        }

        Mockito.when(memberConnector.getRegisteredMemberIds()).thenReturn(membersSet);
        MembersRegistry members = mock(MembersRegistry.class);
        Mockito.when(memberConnector.getActiveMembers()).thenReturn(members);
        Mockito.when(members.getAll()).thenReturn(new HashSet<>());

        raft.consumeRequestVoteResponse(member, requestVoteResponseTerm1);
        assertEquals(1, votesReceived.get());
        assertEquals(0, votesRejected.get());
        assertEquals(MemberRole.Candidate, raft.getRole());

        raft.consumeRequestVoteResponse(member, requestVoteResponseTerm1);
        assertEquals(2, votesReceived.get());
        assertEquals(0, votesRejected.get());
        assertEquals(MemberRole.Candidate, raft.getRole());

        // Send from a different term, should reject.
        raft.consumeRequestVoteResponse(member, requestVoteResponseTerm0);
        assertEquals(2, votesReceived.get());
        assertEquals(1, votesRejected.get());
        assertEquals(MemberRole.Candidate, raft.getRole());

        raft.consumeRequestVoteResponse(member, requestVoteResponseTerm1);
        assertEquals(3, votesReceived.get());
        assertEquals(1, votesRejected.get());
        assertEquals(MemberRole.Candidate, raft.getRole());

        raft.consumeRequestVoteResponse(member, requestVoteResponseTerm1);
        assertEquals(4, votesReceived.get());
        assertEquals(1, votesRejected.get());
        assertEquals(MemberRole.Leader, raft.getRole());

        raft.shutdown();
    }

    @Test
    public void testConsumeVoteRequestResponse_CheckSateTerm()
            throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException,
            SnapshotInstallException, LogException {
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", NetworkUtils.getRandomFreePort())).logUri(URI.create("file:///test")).build();

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = mock(ConsensusEventListener.class);
        Log log = mock(Log.class);
        ActiveMember member = mock(ActiveMember.class);
        Channel senderChannel = mock(Channel.class);
        Mockito.when(member.getChannel()).thenReturn(senderChannel);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, 1, MemberRole.Candidate, raftListener);

        Method initializeEventLoop = ConsensusImpl.class.getDeclaredMethod("initializeEventLoop", new Class[] {});
        initializeEventLoop.setAccessible(true);
        initializeEventLoop.invoke(raft, new Object[] {});

        assertEquals(MemberRole.Candidate, raft.getRole());
        assertEquals(1, raft.getCurrentTerm());

        RequestVoteResponse requestVoteResponseTerm1 = ProtocolMessages.RequestVoteResponse.newBuilder()
                .setTerm(2)
                .setVoteGranted(true)
                .build();

        raft.consumeRequestVoteResponse(member, requestVoteResponseTerm1);
        assertEquals(MemberRole.Follower, raft.getRole());
    }

    @Test(timeout = 30000)
    public void testCheckReplicationRetryTask()
            throws InterruptedException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            FileNotFoundException, SnapshotInstallException, IOException, LogException {
        // Creates 3 members. Sends a command. Waits until a retry has been sent
        // to each of the three members three times.
        ConcurrentHashMap<MemberId, CountDownLatch> latches = new ConcurrentHashMap<>();

        int waitUntilRetryCount = 3;
        int membersCount = 3;

        Set<MemberId> memberIds = new HashSet<MemberId>();
        // One port is left for the leader.
        Iterator<Integer> it = NetworkUtils.getRandomFreePorts(membersCount).iterator();
        for (int i = 0; i < membersCount - 1; ++i) {
            Integer port = it.next();
            MemberId memberId = new MemberId("localhost", port);
            latches.put(memberId, new CountDownLatch(waitUntilRetryCount));
            memberIds.add(memberId);
            it.remove();
        }

        // For the leader last it.next().
        Configuration configuration = Configuration.newBuilder().selfId(new MemberId("localhost", it.next())).addMemberIds(memberIds).logUri(URI.create("file:///test")).build();
        Injector injector = Guice.createInjector(new Module(configuration));

        MemberConnector memberConnector = injector.getInstance(MemberConnector.class);
        Log log = new InMemoryLogImpl();

        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        Mockito.when(messageReceiver.getMemberId()).thenReturn(new MemberId("localhost", NetworkUtils.getRandomFreePort()));

        StateMachine stateMachine = mock(StateMachine.class);
        ConsensusEventListener raftListener = new ConsensusEventListenerImpl() {
            @Override
            public void appendEntriesRetryScheduled(MemberId memberId) {
                latches.get(memberId).countDown();
            }
        };

        final ConsensusImpl raft = new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, 0, MemberRole.Candidate, raftListener);
        Method initializeEventLoop = ConsensusImpl.class.getDeclaredMethod("initializeEventLoop", new Class[] {});
        initializeEventLoop.setAccessible(true);
        initializeEventLoop.invoke(raft, new Object[] {});
        Method becomeLeader = ConsensusImpl.class.getDeclaredMethod("becomeLeader", new Class[] {});
        becomeLeader.setAccessible(true);
        becomeLeader.invoke(raft, new Object[] {});
        raft.start();

        raft.applyCommand(new byte[0]);

        for (CountDownLatch latch : latches.values()) {
            latch.await();
        }
    }
}

package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.messages.ProtocolMessages;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MessageReceiver;

public class RaftTest {
    private AppendEntries getAppendEntriesInstance() {
        AppendEntries requestAppendEntries = ProtocolMessages.AppendEntries.newBuilder()
                .setTerm(1)
                .setLeaderId("test")
                .setPrevLogIndex(0)
                .setLeaderCommitIndex(1)
                .build();
        return requestAppendEntries;
    }

    @Test
    public void checkHeartbeatTimeout() throws InterruptedException {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(150);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);
        Log log = mock(Log.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, log, raftObserver);
        final AtomicLong lastHeartbeatTime = new AtomicLong();
        final AtomicLong requestVotesInitiatedTime = new AtomicLong();

        // Start as a follower with term set to 0.
        assertEquals(Role.Follower, raft.getRole());
        assertEquals(0, raft.getCurrentTerm());

        // Register when was the last heartbeat sent.
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                lastHeartbeatTime.set(System.currentTimeMillis());
                return null;
            }
        }).when(raftObserver).heartbeatReceived();

        // Set a latch on a new election event.
        final CountDownLatch electionInitiatedLatch = new CountDownLatch(1);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                requestVotesInitiatedTime.set(System.currentTimeMillis());
                electionInitiatedLatch.countDown();
                return null;
            }
        }).when(raftObserver).heartbeatTimedout();

        // This will wait until an absence of heartbeats is detected. This
        // event triggers a schedule of the next election. Therefore we
        // immediately cancel it two times by sending desired heartbeats,
        // so as to check that the action on receiving a heartbeat works as
        // expected.
        final CountDownLatch nextElectionScheduledLatch = new CountDownLatch(2);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                if (nextElectionScheduledLatch.getCount() > 0) {
                    nextElectionScheduledLatch.countDown();
                    raft.consumeAppendEntries(null, getAppendEntriesInstance());
                }
                return null;
            }
        }).when(raftObserver).nextElectionScheduled();

        raft.start();

        // This will cancel a new election twice, as two heartbeats will be
        // sent. Then it will initiate a new election.
        nextElectionScheduledLatch.await();
        // Wait until a new election is initiated.
        electionInitiatedLatch.await();

        raft.shutdown();

        assertTrue(raft.getCurrentTerm() > 0);
        assertEquals(Role.Candidate, raft.getRole());

        verify(raftObserver, atLeast(1)).heartbeatReceived();
        verify(raftObserver, atLeast(1)).heartbeatTimedout();
        assertTrue(requestVotesInitiatedTime.get() - lastHeartbeatTime.get() >= configuration.getHeartbeatTimeout());
    }

    @Test
    public void testElectionTimeout() throws InterruptedException {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(150);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);
        Log log = mock(Log.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, log, raftObserver);
        // Start as a follower with term set to 0.
        assertEquals(Role.Follower, raft.getRole());
        assertEquals(0, raft.getCurrentTerm());

        // This set up tests that election time out works. Once a new election
        // has been initiated, we allow it to time out twice. We should be able
        // to see that the term at least 2, and that we are still a Candidate.
        final CountDownLatch electionTimedoutLatch = new CountDownLatch(2);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                electionTimedoutLatch.countDown();
                return null;
            }
        }).when(raftObserver).electionTimedout();

        raft.start();

        electionTimedoutLatch.await();

        raft.shutdown();

        // At least two terms further.
        assertTrue(raft.getCurrentTerm() >= 2);
        assertEquals(Role.Candidate, raft.getRole());
    }
}

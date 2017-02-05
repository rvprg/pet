package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.rvprg.raft.configuration.Configuration;
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
    public void checkRaftHeartbeat() throws InterruptedException {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(500);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, raftObserver);
        final AtomicLong lastHeartbeatTime = new AtomicLong();
        final AtomicLong requestVotesInitiatedTime = new AtomicLong();

        assertEquals(Role.Follower, raft.getRole());

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                lastHeartbeatTime.set(System.currentTimeMillis());
                return null;
            }
        }).when(raftObserver).heartbeatReceived();

        final CountDownLatch requestVotesInitiatedLatch = new CountDownLatch(1);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                requestVotesInitiatedTime.set(System.currentTimeMillis());
                requestVotesInitiatedLatch.countDown();
                return null;
            }
        }).when(raftObserver).electionInitiated();

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

        nextElectionScheduledLatch.await();
        requestVotesInitiatedLatch.await();

        verify(raftObserver, times(3)).nextElectionScheduled();
        verify(raftObserver, times(2)).heartbeatReceived();

        assertEquals(Role.Candidate, raft.getRole());
        assertTrue(requestVotesInitiatedTime.get() - lastHeartbeatTime.get() >= configuration.getHeartbeatTimeout());

        raft.shutdown();
    }
}

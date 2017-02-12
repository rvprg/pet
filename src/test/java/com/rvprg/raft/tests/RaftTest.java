package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.LogEntry;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.messages.ProtocolMessages;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse.Builder;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MessageReceiver;

import io.netty.channel.Channel;

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

    @Test
    public void testConsumeVoteRequest_GiveVoteOnce() {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(150);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);
        Log log = mock(Log.class);
        Channel senderChannel = mock(Channel.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, log, raftObserver);

        assertEquals(0, raft.getCurrentTerm());
        assertEquals(0, log.getCommitIndex());

        RequestVote requestVote0 = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(0)
                .setCandidateId("test")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        LogEntry logEntry = mock(LogEntry.class);
        Mockito.when(logEntry.getTerm()).thenReturn(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.length()).thenReturn(0);

        raft.consumeRequestVote(senderChannel, requestVote0);

        Builder response0 = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse0 = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response0.setVoteGranted(true).setTerm(0).build())
                .build();

        verify(senderChannel, times(1)).writeAndFlush(eq(expectedResponse0));

        // Different candidate.
        RequestVote requestVote1 = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(0)
                .setCandidateId("test1")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        raft.consumeRequestVote(senderChannel, requestVote1);

        Builder response1 = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse1 = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response1.setVoteGranted(false).setTerm(0).build())
                .build();

        verify(senderChannel, times(1)).writeAndFlush(eq(expectedResponse1));
    }

    @Test
    public void testConsumeVoteRequest_GiveVoteSameCandidate() {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(150);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);
        Log log = mock(Log.class);
        Channel senderChannel = mock(Channel.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, log, raftObserver);

        assertEquals(0, raft.getCurrentTerm());
        assertEquals(0, log.getCommitIndex());

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(0)
                .setCandidateId("test")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        LogEntry logEntry = mock(LogEntry.class);
        Mockito.when(logEntry.getTerm()).thenReturn(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.length()).thenReturn(0);

        raft.consumeRequestVote(senderChannel, requestVote);

        Builder response = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(true).setTerm(0).build())
                .build();

        verify(senderChannel, times(1)).writeAndFlush(eq(expectedResponse));

        // Send the same request
        raft.consumeRequestVote(senderChannel, requestVote);

        // Should grant vote.
        verify(senderChannel, times(2)).writeAndFlush(eq(expectedResponse));
    }

    @Test
    public void testConsumeVoteRequest_GiveVoteIfLogIsAsUpToDateAsReceveirs() {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(150);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);
        Log log = mock(Log.class);
        Channel senderChannel = mock(Channel.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, log, raftObserver);

        assertEquals(0, raft.getCurrentTerm());
        assertEquals(0, log.getCommitIndex());

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(1)
                .setCandidateId("test")
                .setLastLogIndex(1)
                .setLastLogTerm(1)
                .build();

        LogEntry logEntry = mock(LogEntry.class);
        Mockito.when(logEntry.getTerm()).thenReturn(0);
        Mockito.when(log.getLast()).thenReturn(logEntry);
        Mockito.when(log.length()).thenReturn(0);

        raft.consumeRequestVote(senderChannel, requestVote);

        Builder response = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(true).setTerm(0).build())
                .build();

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponse));

        // Assume receiver's log is more up to date then no vote should be
        // granted (i.e. last entry has more recent term, 1 != 2)
        Mockito.when(logEntry.getTerm()).thenReturn(2);

        raft.consumeRequestVote(senderChannel, requestVote);

        Builder responseNoVoteGranted = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponseNoVoteGranted = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(responseNoVoteGranted.setVoteGranted(false).setTerm(0).build())
                .build();

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponseNoVoteGranted));

        // Assume receiver's log ends with the same term, but its length is
        // longer than candidates, so no vote should be granted.
        Mockito.when(logEntry.getTerm()).thenReturn(1);
        Mockito.when(log.length()).thenReturn(2);

        raft.consumeRequestVote(senderChannel, requestVote);

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponseNoVoteGranted));
    }

    @Test
    public void testConsumeVoteRequest_DontGiveVoteIfTermIsOutdated() {
        Configuration configuration = new Configuration();
        configuration.setHeartbeatTimeout(150);

        MemberConnector memberConnector = mock(MemberConnector.class);
        MessageReceiver messageReceiver = mock(MessageReceiver.class);
        RaftObserver raftObserver = mock(RaftObserver.class);
        Log log = mock(Log.class);
        Channel senderChannel = mock(Channel.class);

        final RaftImpl raft = new RaftImpl(configuration, memberConnector, messageReceiver, log, 2, raftObserver);

        assertEquals(2, raft.getCurrentTerm());

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setTerm(1)
                .setCandidateId("test")
                .setLastLogIndex(1)
                .setLastLogTerm(1)
                .build();

        raft.consumeRequestVote(senderChannel, requestVote);

        Builder response = RequestVoteResponse.newBuilder();
        RaftMessage expectedResponse = RaftMessage.newBuilder()
                .setType(MessageType.RequestVoteResponse)
                .setRequestVoteResponse(response.setVoteGranted(false).setTerm(2).build())
                .build();

        verify(senderChannel, atLeast(1)).writeAndFlush(eq(expectedResponse));
    }
}
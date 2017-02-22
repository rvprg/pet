package com.rvprg.raft.tests;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.protocol.messages.ProtocolMessages;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.raft.tests.helpers.EchoServer;
import com.rvprg.raft.tests.helpers.MemberConnectorObserverTestableImpl;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;

import io.netty.channel.Channel;

public class MessageDispatcherTest {

    private RaftMessage getRequestVoteInstance() {
        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setCandidateId("test")
                .setTerm(1)
                .setLastLogIndex(2)
                .setLastLogTerm(3)
                .build();
        RaftMessage requestVoteRaftMessage = ProtocolMessages.RaftMessage.newBuilder()
                .setRequestVote(requestVote)
                .setType(MessageType.RequestVote).build();

        return requestVoteRaftMessage;
    }

    private RaftMessage getRequestVoteResponseInstance() {
        RequestVoteResponse requestVoteResponse = ProtocolMessages.RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .build();
        RaftMessage requestVoteResponseRaftMessage = ProtocolMessages.RaftMessage.newBuilder()
                .setRequestVoteResponse(requestVoteResponse)
                .setType(MessageType.RequestVoteResponse).build();
        return requestVoteResponseRaftMessage;
    }

    private RaftMessage getAppendEntriesInstance() {
        AppendEntries requestAppendEntries = ProtocolMessages.AppendEntries.newBuilder()
                .setTerm(1)
                .setLeaderId("test")
                .setPrevLogIndex(0)
                .setLeaderCommitIndex(1)
                .build();
        RaftMessage requestAppendEntriesRaftMessage = ProtocolMessages.RaftMessage.newBuilder()
                .setAppendEntries(requestAppendEntries)
                .setType(MessageType.AppendEntries).build();
        return requestAppendEntriesRaftMessage;
    }

    private RaftMessage getAppendEntriesResponseInstance() {
        AppendEntriesResponse requestAppendEntriesResponse = ProtocolMessages.AppendEntriesResponse.newBuilder()
                .setTerm(1)
                .build();
        RaftMessage requestAppendEntriesRaftMessage = ProtocolMessages.RaftMessage.newBuilder()
                .setAppendEntriesResponse(requestAppendEntriesResponse)
                .setType(MessageType.AppendEntriesResponse).build();
        return requestAppendEntriesRaftMessage;
    }

    private void checkRequestVoteDispatch(MessageConsumer messageConsumer, Channel channel) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(messageConsumer).consumeRequestVote(any(Member.class), any(RequestVote.class));

        RaftMessage requestVoteRaftMessage = getRequestVoteInstance();
        channel.writeAndFlush(requestVoteRaftMessage);

        latch.await();
    }

    private void checkRequestVoteResponseDispatch(MessageConsumer messageConsumer, Channel channel) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(messageConsumer).consumeRequestVoteResponse(any(Member.class), any(RequestVoteResponse.class));

        RaftMessage requestVoteResponseRaftMessage = getRequestVoteResponseInstance();
        channel.writeAndFlush(requestVoteResponseRaftMessage);

        latch.await();

        verify(messageConsumer, times(1)).consumeRequestVoteResponse(any(), eq(requestVoteResponseRaftMessage.getRequestVoteResponse()));
    }

    private void checkAppendEntriesDispatch(MessageConsumer messageConsumer, Channel channel) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(messageConsumer).consumeAppendEntries(any(Member.class), any(AppendEntries.class));

        RaftMessage requestAppendEntriesRaftMessage = getAppendEntriesInstance();
        channel.writeAndFlush(requestAppendEntriesRaftMessage);

        latch.await();

        verify(messageConsumer, times(1)).consumeAppendEntries(any(), eq(requestAppendEntriesRaftMessage.getAppendEntries()));
    }

    private void checkAppendEntriesResponseDispatch(MessageConsumer messageConsumer, Channel channel) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(messageConsumer).consumeAppendEntriesResponse(any(Member.class), any(AppendEntriesResponse.class));

        RaftMessage requestAppendEntriesResponseRaftMessage = getAppendEntriesResponseInstance();
        channel.writeAndFlush(requestAppendEntriesResponseRaftMessage);

        latch.await();

        verify(messageConsumer, times(1)).consumeAppendEntriesResponse(any(), eq(requestAppendEntriesResponseRaftMessage.getAppendEntriesResponse()));
    }

    @Test
    public void testProtocolMessageDispatcher() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module(Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).build()));
        MemberConnector connector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        MemberConnectorObserverTestableImpl observer = new MemberConnectorObserverTestableImpl(messageConsumer, pipelineInitializer);

        EchoServer server = new EchoServer(pipelineInitializer);
        server.start().awaitUninterruptibly();

        MemberId member = new MemberId("localhost", server.getPort());
        connector.register(member, observer);
        connector.connect(member);

        observer.awaitForConnectEvent();

        checkRequestVoteDispatch(messageConsumer, connector.getActiveMembers().get(member).getChannel());
        checkRequestVoteResponseDispatch(messageConsumer, connector.getActiveMembers().get(member).getChannel());
        checkAppendEntriesDispatch(messageConsumer, connector.getActiveMembers().get(member).getChannel());
        checkAppendEntriesResponseDispatch(messageConsumer, connector.getActiveMembers().get(member).getChannel());

        server.shutdown();
    }
}

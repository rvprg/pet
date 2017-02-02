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
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.protocol.messages.ProtocolMessages;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.tests.helpers.EchoServer;
import com.rvprg.raft.tests.helpers.MemberConnectorObserverTestableImpl;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.ChannelPipelineInitializer;

import io.netty.channel.Channel;

public class MessageHandlerTest {
    @Test
    public void testProtocolMessageHandler() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module());
        MemberConnector connector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        MemberConnectorObserverTestableImpl observer = new MemberConnectorObserverTestableImpl(messageConsumer, pipelineInitializer);

        int port = 12345;
        EchoServer server = new EchoServer(pipelineInitializer);
        server.start(port).awaitUninterruptibly();

        MemberId member = new MemberId("localhost", port);
        connector.register(member, observer);
        connector.connect(member);

        observer.awaitForConnectEvent();

        RequestVote requestVote = ProtocolMessages.RequestVote.newBuilder()
                .setCandidateId("test")
                .setTerm(1)
                .setLastLogIndex(2)
                .setLastLogTerm(3)
                .build();
        RaftMessage msg = ProtocolMessages.RaftMessage.newBuilder()
                .setRequestVote(requestVote)
                .setType(MessageType.RequestVote).build();

        final CountDownLatch latch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(messageConsumer).consumeRequestVote(any(Channel.class), any(RequestVote.class));

        connector.getActiveMembers().get(member).getChannel().writeAndFlush(msg);

        latch.await();

        verify(messageConsumer, times(1)).consumeRequestVote(any(), eq(msg.getRequestVote()));

        server.shutdown();
    }
}

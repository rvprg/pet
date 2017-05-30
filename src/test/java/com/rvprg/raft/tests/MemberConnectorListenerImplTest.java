package com.rvprg.raft.tests;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URI;
import java.util.List;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.tests.helpers.EchoServer;
import com.rvprg.raft.tests.helpers.MemberConnectorListenerTestableImpl;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;

public class MemberConnectorListenerImplTest {
    @Test
    public void testListenerAddsHandlersOnConnect() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module(Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build()));
        MemberConnector connector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        MemberConnectorListenerTestableImpl listener = new MemberConnectorListenerTestableImpl(messageConsumer, pipelineInitializer);

        EchoServer server = new EchoServer(pipelineInitializer);
        server.start().awaitUninterruptibly();

        MemberId member = new MemberId("localhost", server.getPort());
        connector.register(member, listener);
        connector.connect(member);

        listener.awaitForConnectEvent();

        List<String> thisNames = connector.getActiveMembers().get(member).getChannel().pipeline().names();
        List<String> otherNames = pipelineInitializer.getHandlerNames();
        for (String name : otherNames) {
            assertTrue(thisNames.contains(name));
        }

        server.shutdown();
    }

}

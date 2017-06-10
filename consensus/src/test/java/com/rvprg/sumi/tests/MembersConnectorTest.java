package com.rvprg.sumi.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.net.URI;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.sumi.Module;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.protocol.MessageConsumer;
import com.rvprg.sumi.tests.helpers.EchoServer;
import com.rvprg.sumi.tests.helpers.MemberConnectorListenerTestableImpl;
import com.rvprg.sumi.transport.ChannelPipelineInitializer;
import com.rvprg.sumi.transport.MemberConnector;
import com.rvprg.sumi.transport.MemberId;

public class MembersConnectorTest {
    @Test
    public void testAutoReconnectionBookkeeping() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module(Configuration.newBuilder().selfId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build()));
        MemberConnector connector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);

        assertEquals(0, connector.getRegisteredMemberIds().size());

        MemberId member = new MemberId("localhost", 1234);
        connector.register(member);

        assertEquals(0, connector.getActiveMembers().getAll().size());
        assertEquals(1, connector.getRegisteredMemberIds().size());
        connector.unregister(member);

        connector.connect(member);
        assertEquals(0, connector.getRegisteredMemberIds().size());
        assertEquals(0, connector.getActiveMembers().getAll().size());

        MemberConnectorListenerTestableImpl listener = new MemberConnectorListenerTestableImpl(mock(MessageConsumer.class), pipelineInitializer);
        connector.register(member, listener);
        assertEquals(1, connector.getRegisteredMemberIds().size());
        assertEquals(0, connector.getActiveMembers().getAll().size());

        connector.connect(member);
        listener.awaitForReconnectEvent();

        connector.unregister(member);
        assertEquals(0, connector.getRegisteredMemberIds().size());

        connector.shutdown();
    }

    @Test
    public void testAutoReconnection() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module(Configuration.newBuilder().selfId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build()));
        MemberConnector connector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);

        EchoServer server1 = new EchoServer(pipelineInitializer);
        server1.start().awaitUninterruptibly();
        EchoServer server2 = new EchoServer(pipelineInitializer);
        server2.start().awaitUninterruptibly();

        MemberConnectorListenerTestableImpl listener1 = new MemberConnectorListenerTestableImpl(mock(MessageConsumer.class), pipelineInitializer);
        MemberConnectorListenerTestableImpl listener2 = new MemberConnectorListenerTestableImpl(mock(MessageConsumer.class), pipelineInitializer);

        MemberId member1 = new MemberId("localhost", server1.getPort());
        MemberId member2 = new MemberId("localhost", server2.getPort());

        connector.register(member1, listener1);
        connector.register(member2, listener2);

        connector.connect(member1);
        listener1.awaitForConnectEvent();

        connector.connect(member2);
        listener2.awaitForConnectEvent();

        assertEquals(2, connector.getActiveMembers().getAll().size());

        int server2Port = server2.getPort();
        server2.shutdown();

        listener2.awaitForDisconnectEvent();
        listener2.awaitForReconnectEvent();

        assertEquals(1, connector.getActiveMembers().getAll().size());
        assertNotNull(connector.getActiveMembers().get(member1));
        assertEquals(member1, connector.getActiveMembers().get(member1).getMemberId());

        server2 = new EchoServer(pipelineInitializer);
        server2.start(server2Port).awaitUninterruptibly();

        listener2.awaitForConnectEvent();

        assertEquals(2, connector.getActiveMembers().getAll().size());

        connector.shutdown();

        server1.shutdown();
        server2.shutdown();

        listener1.awaitForDisconnectEvent();
        listener2.awaitForDisconnectEvent();

        assertEquals(0, connector.getActiveMembers().getAll().size());
    }
}

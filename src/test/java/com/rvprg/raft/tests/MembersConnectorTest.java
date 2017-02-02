package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.tests.helpers.EchoServer;
import com.rvprg.raft.tests.helpers.MemberConnectorObserverTestableImpl;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.ChannelPipelineInitializer;

public class MembersConnectorTest {
    @Test
    public void testAutoReconnectionBookkeeping() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module());
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

        MemberConnectorObserverTestableImpl observer = new MemberConnectorObserverTestableImpl(mock(MessageConsumer.class), pipelineInitializer);
        connector.register(member, observer);
        assertEquals(1, connector.getRegisteredMemberIds().size());
        assertEquals(0, connector.getActiveMembers().getAll().size());

        connector.connect(member);
        observer.awaitForReconnectEvent();

        connector.unregister(member);
        assertEquals(0, connector.getRegisteredMemberIds().size());

        connector.shutdown();
    }

    @Test
    public void testAutoReconnection() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module());
        MemberConnector connector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);

        int port1 = 12345;
        int port2 = 12346;

        EchoServer server1 = new EchoServer(pipelineInitializer);
        server1.start(port1).awaitUninterruptibly();
        EchoServer server2 = new EchoServer(pipelineInitializer);
        server2.start(port2).awaitUninterruptibly();

        MemberConnectorObserverTestableImpl observer1 = new MemberConnectorObserverTestableImpl(mock(MessageConsumer.class), pipelineInitializer);
        MemberConnectorObserverTestableImpl observer2 = new MemberConnectorObserverTestableImpl(mock(MessageConsumer.class), pipelineInitializer);

        MemberId member1 = new MemberId("localhost", port1);
        MemberId member2 = new MemberId("localhost", port2);

        connector.register(member1, observer1);
        connector.register(member2, observer2);

        connector.connect(member1);
        observer1.awaitForConnectEvent();

        connector.connect(member2);
        observer2.awaitForConnectEvent();

        assertEquals(2, connector.getActiveMembers().getAll().size());

        server2.shutdown();

        observer2.awaitForDisconnectEvent();
        observer2.awaitForReconnectEvent();

        assertEquals(1, connector.getActiveMembers().getAll().size());
        assertNotNull(connector.getActiveMembers().get(member1));
        assertEquals(member1, connector.getActiveMembers().get(member1).getMemberId());

        server2 = new EchoServer(pipelineInitializer);
        server2.start(port2).awaitUninterruptibly();

        observer2.awaitForConnectEvent();

        assertEquals(2, connector.getActiveMembers().getAll().size());

        connector.shutdown();

        server1.shutdown();
        server2.shutdown();

        observer1.awaitForDisconnectEvent();
        observer2.awaitForDisconnectEvent();

        assertEquals(0, connector.getActiveMembers().getAll().size());
    }
}

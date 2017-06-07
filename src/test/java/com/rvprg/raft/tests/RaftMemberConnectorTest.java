package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URI;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.protocol.RaftMemberConnector;
import com.rvprg.raft.tests.helpers.EchoServer;
import com.rvprg.raft.tests.helpers.MemberConnectorListenerTestableImpl;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;

public class RaftMemberConnectorTest {
    @Test(timeout = 60000)
    public void testCatchingUpAndVotingMembesBookkeeping() throws InterruptedException {
        Injector injector = Guice.createInjector(new Module(Configuration.newBuilder().selfId(new MemberId("localhost", 1)).logUri(URI.create("file:///test")).build()));
        MemberConnector memberConnector = injector.getInstance(MemberConnector.class);
        ChannelPipelineInitializer pipelineInitializer = injector.getInstance(ChannelPipelineInitializer.class);
        EchoServer server1 = new EchoServer(pipelineInitializer);
        EchoServer server2 = new EchoServer(pipelineInitializer);
        RaftMemberConnector connector = new RaftMemberConnector(memberConnector);

        server1.start().awaitUninterruptibly();
        server2.start().awaitUninterruptibly();

        assertEquals(0, connector.getRegisteredMemberIds().size());

        MemberConnectorListenerTestableImpl listener1 = new MemberConnectorListenerTestableImpl(mock(MessageConsumer.class), pipelineInitializer);
        MemberId member1 = new MemberId("localhost", server1.getPort());
        connector.register(member1, listener1);
        assertEquals(1, connector.getRegisteredMemberIds().size());
        assertEquals(0, connector.getAllActiveMembersCount());
        connector.connect(member1);
        listener1.awaitForConnectEvent();

        MemberConnectorListenerTestableImpl listener2 = new MemberConnectorListenerTestableImpl(mock(MessageConsumer.class), pipelineInitializer);
        MemberId member2 = new MemberId("localhost", server2.getPort());
        connector.registerAsCatchingUpMember(member2, listener2);
        connector.connect(member2);
        listener2.awaitForConnectEvent();

        assertEquals(1, connector.getAllCatchingUpMemberIds().size());
        assertEquals(1, connector.getVotingMembersCount());
        assertEquals(2, connector.getAllActiveMembersCount());
        assertFalse(connector.isCatchingUpMember(member1));
        assertTrue(connector.isCatchingUpMember(member2));

        connector.becomeVotingMember(member2);

        assertEquals(0, connector.getAllCatchingUpMemberIds().size());
        assertEquals(2, connector.getVotingMembersCount());
        assertEquals(2, connector.getAllActiveMembersCount());
        assertFalse(connector.isCatchingUpMember(member1));
        assertFalse(connector.isCatchingUpMember(member2));

        connector.shutdown();

        server1.shutdown();
        server2.shutdown();
    }
}

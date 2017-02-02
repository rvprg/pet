package com.rvprg.raft.tests;

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

public class MemberConnectorObserverImplTest {
    @Test
    public void testObserverAddsHandlersOnConnect() throws InterruptedException {
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

        // connector.getActiveMembers().get(member).getChannel().pipeline().

        server.shutdown();
    }

}

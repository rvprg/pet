package com.rvprg.raft;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.impl.MemberConnectorObserverImpl;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.transport.EditableMembersRegistry;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberConnectorObserver;
import com.rvprg.raft.transport.MembersRegistry;
import com.rvprg.raft.transport.MessageReceiver;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.impl.EditableMembersRegistryImpl;
import com.rvprg.raft.transport.impl.MemberConnectorImpl;
import com.rvprg.raft.transport.impl.MessageReceiverImpl;
import com.rvprg.raft.transport.impl.ChannelPipelineInitializerImpl;

public class Module extends AbstractModule {

    @Override
    protected void configure() {
        // We want the following interfaces to bind to the same instances,
        // hence Singleton.class.
        bind(EditableMembersRegistryImpl.class).in(Singleton.class);
        // Same instance, different interfaces.
        bind(MembersRegistry.class).to(EditableMembersRegistryImpl.class);
        bind(EditableMembersRegistry.class).to(EditableMembersRegistryImpl.class);

        bind(MemberConnector.class).to(MemberConnectorImpl.class);
        bind(MessageReceiver.class).to(MessageReceiverImpl.class);

        bind(MemberConnectorObserver.class).to(MemberConnectorObserverImpl.class);
        bind(ChannelPipelineInitializer.class).to(ChannelPipelineInitializerImpl.class);

        bind(RaftImpl.class).in(Singleton.class);
        bind(MessageConsumer.class).to(RaftImpl.class);
        bind(Raft.class).to(RaftImpl.class);
    }

}

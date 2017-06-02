package com.rvprg.raft;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.log.InMemoryLogImpl;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftImpl;
import com.rvprg.raft.protocol.RaftListener;
import com.rvprg.raft.sm.CommandSerde;
import com.rvprg.raft.sm.CommandSerdeImpl;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.sm.StateMachineImpl;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.ChannelPipelineInitializerImpl;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberConnectorImpl;
import com.rvprg.raft.transport.MemberConnectorListener;
import com.rvprg.raft.transport.MemberConnectorListenerImpl;
import com.rvprg.raft.transport.MembersRegistry;
import com.rvprg.raft.transport.MessageReceiver;
import com.rvprg.raft.transport.MessageReceiverImpl;
import com.rvprg.raft.transport.MutableMembersRegistry;
import com.rvprg.raft.transport.MutableMembersRegistryImpl;

public class Module extends AbstractModule {

    private final Configuration configuration;

    public Module(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(Configuration.class).toInstance(configuration);
        // We want the following interfaces to bind to the same instances,
        // hence Singleton.class.
        bind(MutableMembersRegistryImpl.class).in(Singleton.class);
        // Same instance, different interfaces.
        bind(MembersRegistry.class).to(MutableMembersRegistryImpl.class);
        bind(MutableMembersRegistry.class).to(MutableMembersRegistryImpl.class);

        bind(MemberConnector.class).to(MemberConnectorImpl.class);
        bind(MessageReceiver.class).to(MessageReceiverImpl.class);

        bind(MemberConnectorListener.class).to(MemberConnectorListenerImpl.class);
        bind(ChannelPipelineInitializer.class).to(ChannelPipelineInitializerImpl.class);

        bind(RaftImpl.class).in(Singleton.class);
        bind(MessageConsumer.class).to(RaftImpl.class);
        bind(Raft.class).to(RaftImpl.class);
        bind(Log.class).to(InMemoryLogImpl.class);
        bind(StateMachine.class).to(StateMachineImpl.class);

        bind(CommandSerde.class).to(CommandSerdeImpl.class);

        bind(RaftListener.class).toInstance(RaftListener.getDefaultInstance());
    }

}

package com.rvprg.sumi;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.log.InMemoryLogImpl;
import com.rvprg.sumi.log.Log;
import com.rvprg.sumi.protocol.MessageConsumer;
import com.rvprg.sumi.protocol.Consensus;
import com.rvprg.sumi.protocol.ConsensusImpl;
import com.rvprg.sumi.protocol.ConsensusEventListener;
import com.rvprg.sumi.sm.CommandSerde;
import com.rvprg.sumi.sm.CommandSerdeImpl;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.sm.StateMachineImpl;
import com.rvprg.sumi.transport.ChannelPipelineInitializer;
import com.rvprg.sumi.transport.ChannelPipelineInitializerImpl;
import com.rvprg.sumi.transport.MemberConnector;
import com.rvprg.sumi.transport.MemberConnectorImpl;
import com.rvprg.sumi.transport.MemberConnectorListener;
import com.rvprg.sumi.transport.MemberConnectorListenerImpl;
import com.rvprg.sumi.transport.MembersRegistry;
import com.rvprg.sumi.transport.MessageReceiver;
import com.rvprg.sumi.transport.MessageReceiverImpl;
import com.rvprg.sumi.transport.MutableMembersRegistry;
import com.rvprg.sumi.transport.MutableMembersRegistryImpl;

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

        bind(ConsensusImpl.class).in(Singleton.class);
        bind(MessageConsumer.class).to(ConsensusImpl.class);
        bind(Consensus.class).to(ConsensusImpl.class);
        bind(Log.class).to(InMemoryLogImpl.class);
        bind(StateMachine.class).to(StateMachineImpl.class);

        bind(CommandSerde.class).to(CommandSerdeImpl.class);

        bind(ConsensusEventListener.class).toInstance(ConsensusEventListener.getDefaultInstance());
    }

}

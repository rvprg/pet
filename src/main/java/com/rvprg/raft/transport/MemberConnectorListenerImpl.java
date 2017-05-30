package com.rvprg.raft.transport;

import com.google.inject.Inject;
import com.rvprg.raft.protocol.MessageConsumer;

import net.jcip.annotations.Immutable;

@Immutable
public class MemberConnectorListenerImpl implements MemberConnectorListener {
    private final MessageConsumer messageConsumer;
    private final ChannelPipelineInitializer pipelineInitializer;

    @Inject
    public MemberConnectorListenerImpl(MessageConsumer messageConsumer, ChannelPipelineInitializer pipelineInitializer) {
        this.messageConsumer = messageConsumer;
        this.pipelineInitializer = pipelineInitializer;
    }

    @Override
    public void connected(Member member) {
        pipelineInitializer.initialize(member.getChannel().pipeline()).addLast(new MessageDispatcher(member, messageConsumer));
    }

    @Override
    public void scheduledReconnect(MemberId member) {
        // nop
    }

    @Override
    public void disconnected(MemberId memberId) {
        // nop
    }

    @Override
    public void exceptionCaught(MemberId memberId, Throwable cause) {
        // nop
    }

}

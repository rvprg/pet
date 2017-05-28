package com.rvprg.raft.protocol;

import com.google.inject.Inject;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnectorObserver;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageDispatcher;

import net.jcip.annotations.Immutable;

@Immutable
public class MemberConnectorObserverImpl implements MemberConnectorObserver {
    private final MessageConsumer messageConsumer;
    private final ChannelPipelineInitializer pipelineInitializer;

    @Inject
    public MemberConnectorObserverImpl(MessageConsumer messageConsumer, ChannelPipelineInitializer pipelineInitializer) {
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

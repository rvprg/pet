package com.rvprg.sumi.transport;

import com.google.inject.Inject;
import com.rvprg.sumi.protocol.MessageConsumer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final MessageConsumer messageConsumer;
    private final ChannelPipelineInitializer pipelineInitializer;

    @Inject
    public ServerChannelInitializer(MessageConsumer messageConsumer, ChannelPipelineInitializer pipelineInitializer) {
        this.messageConsumer = messageConsumer;
        this.pipelineInitializer = pipelineInitializer;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        pipelineInitializer.initialize(ch.pipeline()).addLast(new MessageDispatcher(new ActiveMember(ch), messageConsumer));
    }

}

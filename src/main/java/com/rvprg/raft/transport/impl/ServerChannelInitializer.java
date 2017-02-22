package com.rvprg.raft.transport.impl;

import com.google.inject.Inject;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.Member;

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
    protected void initChannel(SocketChannel ch) throws Exception {
        pipelineInitializer.initialize(ch.pipeline()).addLast(new MessageDispatcher(new Member(ch), messageConsumer));
    }

}

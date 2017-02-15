package com.rvprg.raft.transport.impl;

import com.google.inject.Inject;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageReceiver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class MessageReceiverImpl implements MessageReceiver {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap server;
    private final Configuration configuration;
    private final ChannelPipelineInitializer pipelineInitializer;
    private final MemberId id;

    @Inject
    public MessageReceiverImpl(Configuration configuration, ChannelPipelineInitializer pipelineInitializer) {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        this.server = new ServerBootstrap();
        this.configuration = configuration;
        this.pipelineInitializer = pipelineInitializer;
        this.id = new MemberId(configuration.getHost(), configuration.getPort());
    }

    @Override
    public void shutdown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Override
    public void start(final MessageConsumer messageConsumer) throws InterruptedException {
        server.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ServerChannelInitializer(messageConsumer, pipelineInitializer))
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        server.bind(
                configuration.getHost(),
                configuration.getPort()).sync();
    }

    @Override
    public String getId() {
        return id.toString();
    }

    @Override
    public ChannelPipelineInitializer getChannelPipelineInitializer() {
        return pipelineInitializer;
    }

}

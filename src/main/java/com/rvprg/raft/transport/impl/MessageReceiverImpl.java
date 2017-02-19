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
import net.jcip.annotations.GuardedBy;

public class MessageReceiverImpl implements MessageReceiver {
    private final ServerBootstrap server;
    private final Configuration configuration;
    private final ChannelPipelineInitializer pipelineInitializer;
    private final MemberId memberId;

    private final Object stateLock = new Object();
    @GuardedBy("stateLock")
    private EventLoopGroup bossGroup;
    @GuardedBy("stateLock")
    private EventLoopGroup workerGroup;
    @GuardedBy("stateLock")
    private boolean started = false;

    @Inject
    public MessageReceiverImpl(Configuration configuration, ChannelPipelineInitializer pipelineInitializer) {
        this.server = new ServerBootstrap();
        this.configuration = configuration;
        this.pipelineInitializer = pipelineInitializer;
        this.memberId = configuration.getMemberId();
    }

    private void createEventLoops() {
        synchronized (stateLock) {
            this.bossGroup = new NioEventLoopGroup();
            this.workerGroup = new NioEventLoopGroup();
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        synchronized (stateLock) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            started = false;
        }
    }

    @Override
    public void start(final MessageConsumer messageConsumer) throws InterruptedException {
        synchronized (stateLock) {
            if (started) {
                throw new IllegalStateException("MessageReceiver is already started");
            }

            createEventLoops();

            server.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ServerChannelInitializer(messageConsumer, pipelineInitializer))
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            server.bind(configuration.getMemberId()).sync();

            started = true;
        }

    }

    @Override
    public ChannelPipelineInitializer getChannelPipelineInitializer() {
        return pipelineInitializer;
    }

    @Override
    public MemberId getMemberId() {
        return memberId;
    }

    @Override
    public boolean isStarted() {
        synchronized (stateLock) {
            return started;
        }
    }

}

package com.rvprg.raft.tests.helpers;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.transport.ChannelPipelineInitializer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class EchoServer {
    private final int MAX_PORT = 65535;
    private final int MIN_PORT = MAX_PORT / 2;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap server;
    private final ChannelPipelineInitializer pipelineInitializer;
    private final Random random;
    private int serverPort;

    public int getPort() {
        return serverPort;
    }

    private static class EchoHandler extends SimpleChannelInboundHandler<RaftMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
            ctx.channel().writeAndFlush(msg);
        }
    }

    public EchoServer(ChannelPipelineInitializer pipelineInitializer) {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        this.server = new ServerBootstrap();
        this.pipelineInitializer = pipelineInitializer;
        this.random = new Random();
    }

    public void shutdown() {
        // First shutdown boss group.
        bossGroup.shutdownGracefully().awaitUninterruptibly();
        workerGroup.shutdownGracefully().awaitUninterruptibly();
    }

    private boolean isPortFree(int port) {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                }
            }
        }
        return false;
    }

    private int getRandomPort() {
        return random.nextInt(MAX_PORT - MIN_PORT) + MIN_PORT;
    }

    private int getFreePort() {
        int port = 0;
        boolean found = false;
        do {
            port = getRandomPort();
            found = isPortFree(port);
        } while (!found);
        return port;
    }

    public ChannelFuture start() throws InterruptedException {
        return start(getFreePort());
    }

    public ChannelFuture start(int port) throws InterruptedException {
        serverPort = port;
        server.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        pipelineInitializer.initialize(ch.pipeline()).addLast(new EchoHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        return server.bind(port).sync();
    }

}

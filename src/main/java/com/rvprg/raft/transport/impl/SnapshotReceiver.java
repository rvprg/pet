package com.rvprg.raft.transport.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.CompletableFuture;

import com.rvprg.raft.transport.MemberId;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class SnapshotReceiver {
    private final EventLoopGroup workerGroup;
    private final Bootstrap client;
    private final File fileName;
    private final CompletableFuture<Boolean> completionFuture = new CompletableFuture<Boolean>();
    private final MemberId memberId;

    private class SnapshotReceiveHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private BufferedOutputStream out;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            out = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            msg.readBytes(out, msg.readableBytes());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            out.close();
            completionFuture.complete(true);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            completionFuture.completeExceptionally(cause);
            ctx.channel().close();
        }
    }

    public SnapshotReceiver(MemberId memberId, File fileName) {
        this.workerGroup = new NioEventLoopGroup();
        this.client = new Bootstrap();
        this.fileName = fileName;
        this.memberId = memberId;
    }

    public void shutdown() {
        workerGroup.shutdownGracefully().awaitUninterruptibly();
    }

    public CompletableFuture<Boolean> start() throws InterruptedException {
        client.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new SnapshotReceiveHandler());
                    }
                })
                .option(ChannelOption.SO_KEEPALIVE, true);

        client.connect(memberId).sync();
        return completionFuture;
    }

}

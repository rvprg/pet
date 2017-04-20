package com.rvprg.raft.transport.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.CompletableFuture;

import com.rvprg.raft.transport.MemberId;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
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
    private final CompletableFuture<Boolean> completionFuture = new CompletableFuture<Boolean>();
    private final Channel channel;

    private class SnapshotReceiveHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private BufferedOutputStream out;
        private final File fileName;

        public SnapshotReceiveHandler(File fileName) {
            this.fileName = fileName;
        }

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
            if (out != null) {
                out.close();
            }
            System.out.println("done");
            completionFuture.complete(true);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            completionFuture.completeExceptionally(cause);
            if (out != null) {
                out.close();
            }
            ctx.channel().close();
        }
    }

    public SnapshotReceiver(MemberId memberId, File fileName) throws InterruptedException {
        this.workerGroup = new NioEventLoopGroup();

        channel = new Bootstrap().group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new SnapshotReceiveHandler(fileName));
                    }
                })
                .option(ChannelOption.SO_KEEPALIVE, true)
                .connect(memberId)
                .sync()
                .channel();
    }

    public void shutdown() {
        if (channel != null) {
            channel.close();
        }
        workerGroup.shutdownGracefully().awaitUninterruptibly();
    }

    public CompletableFuture<Boolean> getCompletionFuture() {
        return completionFuture;
    }

}

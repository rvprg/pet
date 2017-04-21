package com.rvprg.raft.transport.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.protocol.messages.ProtocolMessages.SnapshotDownloadRequest;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.MemberId;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class SnapshotReceiver {
    private final Logger logger = LoggerFactory.getLogger(SnapshotReceiver.class);

    private final EventLoopGroup workerGroup;
    private final CompletableFuture<Boolean> completionFuture = new CompletableFuture<Boolean>();
    private final Channel channel;
    private final MemberId selfId;
    private final ChannelPipelineInitializer channelPipelineInitializer;

    private class SnapshotReceiveHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private BufferedOutputStream out;
        private final File fileName;
        private final String snapshotId;
        private final long size;
        private long received;
        private long lastLogStatusTime;
        private final static long logStatusInterval = 1000;

        public SnapshotReceiveHandler(String snapshotId, File fileName, long size) {
            this.fileName = fileName;
            this.snapshotId = snapshotId;
            this.size = size;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("SnapshotId={}, requesting download. Size={}.", snapshotId, size);
            ctx.channel().writeAndFlush(RaftMessage.newBuilder().setType(MessageType.SnapshotDownloadRequest)
                    .setSnapshotDownloadRequest(
                            SnapshotDownloadRequest.newBuilder().setMemberId(selfId.toString()).setSnapshotId(snapshotId).setSize(size))
                    .build()).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            for (String handlerName : channelPipelineInitializer.getHandlerNames()) {
                                ctx.pipeline().remove(handlerName);
                            }
                        }
                    });
            out = new BufferedOutputStream(new FileOutputStream(fileName));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            received += msg.readableBytes();
            msg.readBytes(out, msg.readableBytes());

            boolean isFinished = received == size;

            if (System.currentTimeMillis() - lastLogStatusTime > logStatusInterval || isFinished) {
                logger.info("SnapshotId={}, download progress={}%, {}/{}.", snapshotId, ((int) (((double) received / (double) size) * 100.0)), received, size);
                lastLogStatusTime = System.currentTimeMillis();
            }

            if (isFinished) {
                ctx.channel().close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (out != null) {
                out.close();
            }
            completionFuture.complete(true);
            if (received != size) {
                logger.error("SnapshotId={}, download didn't finish, {}/{}.", snapshotId, received, size);
            } else {
                logger.info("SnapshotId={}, download finished. Stored in {}.", snapshotId, fileName);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            completionFuture.completeExceptionally(cause);
            if (out != null) {
                out.close();
            }
            ctx.channel().close();
            logger.error("SnapshotId={}, download has not finished due to an error.", snapshotId, cause);
        }
    }

    public SnapshotReceiver(ChannelPipelineInitializer channelPipelineInitializer,
            MemberId selfId, MemberId memberId, String snapshotId, File fileName, long size) throws InterruptedException {
        this.workerGroup = new NioEventLoopGroup();
        this.selfId = selfId;
        this.channelPipelineInitializer = channelPipelineInitializer;

        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive.");
        }

        channel = new Bootstrap().group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        channelPipelineInitializer.initialize(ch.pipeline())
                                .addLast(new SnapshotReceiveHandler(snapshotId, fileName, size));
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

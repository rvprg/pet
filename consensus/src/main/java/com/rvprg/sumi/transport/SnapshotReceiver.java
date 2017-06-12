package com.rvprg.sumi.transport;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rvprg.sumi.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.SnapshotDownloadRequest;

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
    private final BiConsumer<SnapshotDescriptor, Throwable> handler;
    private final Channel channel;
    private final MemberId selfId;
    private final ChannelPipelineInitializer channelPipelineInitializer;
    private final AtomicBoolean done = new AtomicBoolean(false);

    private class SnapshotReceiveHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private OutputStream out;
        private long received;
        private long lastLogStatusTime;
        private final static long logStatusInterval = 1000;
        private final SnapshotDescriptor snapshotDescriptor;

        public SnapshotReceiveHandler(SnapshotDescriptor snapshotDescriptor) {
            this.snapshotDescriptor = snapshotDescriptor;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Snapshot={}, requesting download. ", snapshotDescriptor);
            ctx.channel().writeAndFlush(RaftMessage.newBuilder().setType(MessageType.SnapshotDownloadRequest)
                    .setSnapshotDownloadRequest(
                            SnapshotDownloadRequest.newBuilder()
                                    .setMemberId(selfId.toString())
                                    .setSnapshotId(snapshotDescriptor.getMetadata().getSnapshotId())
                                    .setTerm(snapshotDescriptor.getMetadata().getTerm())
                                    .setIndex(snapshotDescriptor.getMetadata().getIndex())
                                    .setSize(snapshotDescriptor.getMetadata().getSize()))
                    .build()).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            for (String handlerName : channelPipelineInitializer.getHandlerNames()) {
                                ctx.pipeline().remove(handlerName);
                            }
                        }
                    });
            out = new BufferedOutputStream(new FileOutputStream(snapshotDescriptor.getSnapshotFile(), false));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            received += msg.readableBytes();
            msg.readBytes(out, msg.readableBytes());

            long size = snapshotDescriptor.getMetadata().getSize();
            boolean isFinished = received == size;

            if (System.currentTimeMillis() - lastLogStatusTime > logStatusInterval || isFinished) {
                logger.info("Snapshot={}, download progress={}%, {}/{}.", snapshotDescriptor, ((int) ((received / (double) size) * 100.0)), received, size);
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
            done.set(true);
            if (received != snapshotDescriptor.getMetadata().getSize()) {
                logger.error("Snapshot={}, download didn't finish, {}/{}.", snapshotDescriptor, received,
                        snapshotDescriptor.getMetadata().getSize());
                handler.accept(null, new IllegalStateException("Download didn't finish"));
            } else {
                logger.info("Snapshot={}, download finished. Stored in {}.", snapshotDescriptor, snapshotDescriptor.getSnapshotFile().getPath());
                handler.accept(snapshotDescriptor, null);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            done.set(true);
            handler.accept(null, cause);
            if (out != null) {
                out.close();
            }
            ctx.channel().close();
            logger.error("Snapshot={}, download has not finished due to an error.", snapshotDescriptor, cause);
        }
    }

    public SnapshotReceiver(ChannelPipelineInitializer channelPipelineInitializer,
            MemberId selfId, MemberId memberId, SnapshotDescriptor snapshotDescriptor,
            BiConsumer<SnapshotDescriptor, Throwable> handler) throws InterruptedException {
        this.workerGroup = new NioEventLoopGroup();
        this.selfId = selfId;
        this.channelPipelineInitializer = channelPipelineInitializer;
        this.handler = handler;

        if (snapshotDescriptor.getMetadata().getSize() <= 0) {
            throw new IllegalArgumentException("Size must be positive.");
        }

        channel = new Bootstrap().group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        channelPipelineInitializer.initialize(ch.pipeline())
                                .addLast(new SnapshotReceiveHandler(snapshotDescriptor));
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
        // FIXME:
        workerGroup.shutdownGracefully().awaitUninterruptibly();
    }

    public boolean isDone() {
        return done.get();
    }
}

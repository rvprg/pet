package com.rvprg.sumi.transport;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedWriteHandler;

public class SnapshotSender {
    private final Logger logger = LoggerFactory.getLogger(SnapshotSender.class);

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap server;

    private final AtomicReference<SnapshotDescriptor> snapshotDescriptor;
    private final Consumer<SnapshotTransferEvent> eventCallback;
    private final ChannelPipelineInitializer channelPipelineInitializer;

    private final MemberId selfId;

    private class SnapshotTransferInitiator extends SimpleChannelInboundHandler<RaftMessage> {
        private volatile MemberId memberId;
        public static final String SnapshotTransferInitiator = "SnapshotTransferInitiator";
        private static final String ChunkedWriteHandler = "ChunkedWriteHandler";
        private SnapshotDescriptor snapshot;

        private void initMember(ChannelHandlerContext ctx) {
            if (memberId == null) {
                memberId = new MemberId(ctx.channel().remoteAddress());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            initMember(ctx);
            eventCallback.accept(new SnapshotTransferExceptionThrownEvent(memberId, ctx.channel(), snapshot, cause));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            initMember(ctx);
            eventCallback.accept(new SnapshotTransferConnectionClosedEvent(memberId, ctx.channel(), snapshot));
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            snapshot = SnapshotSender.this.snapshotDescriptor.get();
            if (snapshot == null) {
                ctx.channel().close();
                return;
            }
            initMember(ctx);
            eventCallback.accept(new SnapshotTransferConnectionOpenedEvent(memberId, ctx.channel(), snapshot));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
            if (msg.getType() != MessageType.SnapshotDownloadRequest) {
                logger.error("Wrong message type. Closing connection.");
                ctx.channel().close();
                return;
            }

            String memberIdStr = msg.getSnapshotDownloadRequest().getMemberId();
            try {
                memberId = MemberId.fromString(memberIdStr);
            } catch (IllegalArgumentException e) {
                logger.error("Could not parse memberId. MemberId string = {}. Closing the connection.", memberIdStr, e);
                ctx.channel().close();
                return;
            }

            String snapshotId = msg.getSnapshotDownloadRequest().getSnapshotId();
            if (!snapshot.getMetadata().getSnapshotId().equalsIgnoreCase(snapshotId)) {
                logger.info("MemberId={} (as reported). Requested SnapshotId={}, but we are serving snapshotId={}. Closing the connection.", memberIdStr, snapshotId,
                        snapshot.getMetadata().getSnapshotId());
                ctx.channel().close();
                return;
            }

            for (String handlerName : channelPipelineInitializer.getHandlerNames()) {
                ctx.pipeline().remove(handlerName);
            }
            ctx.pipeline().addBefore(SnapshotTransferInitiator, ChunkedWriteHandler, new ChunkedWriteHandler());

            final ChunkedFile chunkedFile = new ChunkedFile(snapshot.getSnapshotFile());
            ctx.channel().writeAndFlush(chunkedFile).addListener(ChannelFutureListener.CLOSE).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    chunkedFile.close();
                    eventCallback.accept(new SnapshotTransferCompletedEvent(memberId, ctx.channel(), snapshot));
                }
            });

            eventCallback.accept(new SnapshotTransferStartedEvent(memberId, ctx.channel(), snapshot));
        }
    }

    public SnapshotSender(ChannelPipelineInitializer channelPipelineInitializer,
            MemberId selfId,
            Consumer<SnapshotTransferEvent> eventCallback) throws InterruptedException {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        this.server = new ServerBootstrap();
        this.snapshotDescriptor = new AtomicReference<SnapshotDescriptor>(null);
        this.eventCallback = eventCallback;
        this.channelPipelineInitializer = channelPipelineInitializer;
        this.selfId = selfId;

        server.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        channelPipelineInitializer.initialize(ch.pipeline()).addLast(
                                SnapshotTransferInitiator.SnapshotTransferInitiator, new SnapshotTransferInitiator());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    public void start() throws InterruptedException {
        server.bind(selfId).sync();
    }

    public void setSnapshotDescriptor(SnapshotDescriptor snapshot) {
        this.snapshotDescriptor.set(snapshot);
    }

    public SnapshotDescriptor getSnapshotDescriptor() {
        return this.snapshotDescriptor.get();
    }

    public void shutdown() {
        // FIXME:
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public MemberId getMemberId() {
        return selfId;
    }

}

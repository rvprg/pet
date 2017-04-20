package com.rvprg.raft.transport.impl;

import java.io.File;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage.MessageType;
import com.rvprg.raft.transport.ChannelPipelineInitializer;
import com.rvprg.raft.transport.MemberId;

import io.netty.bootstrap.ServerBootstrap;
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
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedWriteHandler;

public class SnapshotSender {
    private final Logger logger = LoggerFactory.getLogger(SnapshotSender.class);

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap server;
    private final File fileName;
    private final String snapshotId;

    private final BiConsumer<MemberId, Channel> transferStart;
    private final Consumer<MemberId> transferComplete;
    private final BiConsumer<MemberId, Throwable> transferCompleteExceptionally;

    private final ChannelPipelineInitializer channelPipelineInitializer;

    private class SnapshotTransferInitiator extends SimpleChannelInboundHandler<RaftMessage> {
        private MemberId memberId;
        public static final String SnapshotTransferInitiator = "SnapshotTransferInitiator";
        private static final String ChunkedWriteHandler = "ChunkedWriteHandler";

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            transferCompleteExceptionally.accept(memberId, cause);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
            if (msg.getType() != MessageType.SnapshotDownloadRequest) {
                logger.error("Wrong message type. Ignoring.");
                return;
            }

            String memberIdStr = msg.getSnapshotDownloadRequest().getMemberId();
            try {
                memberId = MemberId.fromString(memberIdStr);
            } catch (IllegalArgumentException e) {
                logger.error("Could not parse memberId. {}", memberIdStr, e);
                return;
            }

            String snapshotId = msg.getSnapshotDownloadRequest().getSnapshotId();
            if (!SnapshotSender.this.snapshotId.equalsIgnoreCase(snapshotId)) {
                ctx.channel().close();
                logger.info("MemberId={}. Requested snapshotId={}, but we are serving snapshotId={}. Closing connection.", memberIdStr, snapshotId, SnapshotSender.this.snapshotId);
                return;
            }

            for (String handlerName : channelPipelineInitializer.getHandlerNames()) {
                ctx.pipeline().remove(handlerName);
            }
            ctx.pipeline().addBefore(SnapshotTransferInitiator, ChunkedWriteHandler, new ChunkedWriteHandler());

            final ChunkedFile chunkedFile = new ChunkedFile(fileName);
            ctx.channel().writeAndFlush(chunkedFile).addListener(ChannelFutureListener.CLOSE).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    chunkedFile.close();
                    transferComplete.accept(memberId);
                }
            });

            transferStart.accept(memberId, ctx.channel());
        }
    }

    public SnapshotSender(ChannelPipelineInitializer channelPipelineInitializer,
            MemberId memberId, String snapshotId, File fileName,
            BiConsumer<MemberId, Channel> transferStart,
            Consumer<MemberId> transferComplete,
            BiConsumer<MemberId, Throwable> transferCompleteExceptionally) throws InterruptedException {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        this.server = new ServerBootstrap();
        this.fileName = fileName;
        this.snapshotId = snapshotId;
        this.transferStart = transferStart;
        this.transferComplete = transferComplete;
        this.transferCompleteExceptionally = transferCompleteExceptionally;
        this.channelPipelineInitializer = channelPipelineInitializer;

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

        server.bind(memberId).sync();
    }

    public void shutdown() {
        bossGroup.shutdownGracefully().awaitUninterruptibly();
        workerGroup.shutdownGracefully().awaitUninterruptibly();
    }

}

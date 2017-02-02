package com.rvprg.raft.transport.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MessageDispatcher extends SimpleChannelInboundHandler<RaftMessage> {
    private final static Logger logger = LoggerFactory.getLogger(MessageDispatcher.class);

    private final MessageConsumer messageConsumer;

    @Inject
    public MessageDispatcher(final MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        ctx.channel().close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
        switch (msg.getType()) {
        case AppendEntries:
            messageConsumer.consumeAppendEntries(ctx.channel(), msg.getAppendEntries());
            break;
        case AppendEntriesResponse:
            messageConsumer.consumeAppendEntriesResponse(ctx.channel(), msg.getAppendEntriesResponse());
            break;
        case RequestVote:
            messageConsumer.consumeRequestVote(ctx.channel(), msg.getRequestVote());
            break;
        case RequestVoteResponse:
            messageConsumer.consumeRequestVoteResponse(ctx.channel(), msg.getRequestVoteResponse());
            break;
        default:
            logger.warn("Unknown message received from {}. Ignored.", ctx.channel().remoteAddress());
        }
    }

}

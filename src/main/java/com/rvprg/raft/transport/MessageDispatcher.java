package com.rvprg.raft.transport;

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
    private final Member member;

    @Inject
    public MessageDispatcher(final Member member, final MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
        this.member = member;
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
            messageConsumer.consumeAppendEntries(member, msg.getAppendEntries());
            break;
        case AppendEntriesResponse:
            messageConsumer.consumeAppendEntriesResponse(member, msg.getAppendEntriesResponse());
            break;
        case RequestVote:
            messageConsumer.consumeRequestVote(member, msg.getRequestVote());
            break;
        case RequestVoteResponse:
            messageConsumer.consumeRequestVoteResponse(member, msg.getRequestVoteResponse());
            break;
        default:
            logger.warn("Unknown message received from {}. Ignored.", ctx.channel().remoteAddress());
        }
    }

}

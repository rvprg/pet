package com.rvprg.sumi.transport;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class MemberRegistryHandler extends ChannelInboundHandlerAdapter {
    private final static Logger logger = LoggerFactory.getLogger(MemberRegistryHandler.class);

    private final MutableMembersRegistry registry;
    private final MemberConnector memberConnector;
    private final BiConsumer<ActiveMember, Channel> activateNotifier;
    private final Consumer<MemberId> deactivateNotifier;
    private final BiConsumer<MemberId, Throwable> exceptionNotifier;

    @Inject
    public MemberRegistryHandler(
            MutableMembersRegistry members,
            MemberConnector memberConnector,
            BiConsumer<ActiveMember, Channel> activateNotifier,
            Consumer<MemberId> deactivateNotifier,
            BiConsumer<MemberId, Throwable> exceptionNotifier) {
        this.registry = members;
        this.memberConnector = memberConnector;
        this.activateNotifier = activateNotifier;
        this.deactivateNotifier = deactivateNotifier;
        this.exceptionNotifier = exceptionNotifier;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ActiveMember member = new ActiveMember(ctx.channel());
        registry.addMember(member);
        activateNotifier.accept(member, ctx.channel());
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ActiveMember member = new ActiveMember(ctx.channel());
        registry.removeMember(member);
        deactivateNotifier.accept(member.getMemberId());
        memberConnector.connect(member.getMemberId());
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        ActiveMember member = new ActiveMember(ctx.channel());
        registry.removeMember(member);
        exceptionNotifier.accept(member.getMemberId(), cause);
        logger.error("Error in the channel to {}: {}", member, cause.getMessage());
        ctx.channel().close();
    }
}

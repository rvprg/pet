package com.rvprg.raft.transport.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.rvprg.raft.transport.EditableMembersRegistry;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberConnectorObserver;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MembersRegistry;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class MemberConnectorImpl implements MemberConnector {
    private final static Logger logger = LoggerFactory.getLogger(MemberConnectorImpl.class);

    private final ConcurrentHashMap<MemberId, MemberConnectorObserver> registered = new ConcurrentHashMap<MemberId, MemberConnectorObserver>();
    private final int retryDelay = 1000;

    private final Bootstrap clientBootstrap;

    private final EventLoopGroup workerGroup;
    private final MembersRegistry membersRegistry;

    @Override
    public MembersRegistry getActiveMembers() {
        return membersRegistry;
    }

    @Inject
    public MemberConnectorImpl(final EditableMembersRegistry members) {
        this.membersRegistry = members;
        clientBootstrap = new Bootstrap();
        workerGroup = new NioEventLoopGroup();
        clientBootstrap.group(workerGroup);
        clientBootstrap.channel(NioSocketChannel.class);
        clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new MemberRegistryHandler(
                        members,
                        MemberConnectorImpl.this,
                        member -> MemberConnectorImpl.this.memberActivated(member),
                        member -> MemberConnectorImpl.this.memberDeactivated(member),
                        (member, cause) -> MemberConnectorImpl.this.memberExceptionCaught(member, cause)));
            }
        });
    }

    @Override
    public void register(MemberId member) {
        register(member, null);
    }

    @Override
    public void register(final MemberId member, final MemberConnectorObserver observer) {
        registered.putIfAbsent(member, observer == null ? MemberConnectorObserver.getDefaultObserverInstance() : observer);
    }

    @Override
    public void unregister(MemberId member) {
        registered.remove(member);
    }

    @Override
    public Set<MemberId> getRegisteredMemberIds() {
        return ImmutableSet.copyOf(registered.keySet());
    }

    @Override
    public void connect(final MemberId memberId) {
        if (!registered.containsKey(memberId)) {
            return;
        }

        if (isShutdown()) {
            return;
        }

        final ChannelFuture channelFuture = clientBootstrap.connect(memberId);
        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Connection to {} failed. Retrying in {} ms.", memberId, retryDelay);
                    memberScheduledReconnect(memberId);

                    channelFuture.channel().eventLoop().schedule(new Runnable() {
                        @Override
                        public void run() {
                            connect(memberId);
                        }
                    }, retryDelay, TimeUnit.MILLISECONDS);
                }
            }
        });
    }

    private void memberActivated(Member member) {
        MemberConnectorObserver observer = registered.get(member.getMemberId());
        if (observer != null) {
            observer.connected(member);
        }
    }

    private void memberScheduledReconnect(MemberId memberId) {
        MemberConnectorObserver observer = registered.get(memberId);
        if (observer != null) {
            observer.scheduledReconnect(memberId);
        }
    }

    private void memberDeactivated(MemberId memberId) {
        MemberConnectorObserver observer = registered.get(memberId);
        if (observer != null) {
            observer.disconnected(memberId);
        }
    }

    private void memberExceptionCaught(MemberId memberId, Throwable cause) {
        MemberConnectorObserver observer = registered.get(memberId);
        if (observer != null) {
            observer.exceptionCaught(memberId, cause);
        }
    }

    @Override
    public void shutdown() {
        workerGroup.shutdownGracefully();
    }

    @Override
    public boolean isShutdown() {
        return workerGroup.isShuttingDown();
    }

}

package com.rvprg.sumi.transport;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.rvprg.sumi.configuration.Configuration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
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

    private final ConcurrentHashMap<MemberId, MemberConnectorListener> registered = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MemberId, Channel> registeredChannels = new ConcurrentHashMap<>();

    private ImmutableSet<MemberId> immutableMemberIdSet = ImmutableSet.of();
    private final Bootstrap clientBootstrap;

    private final EventLoopGroup workerGroup;
    private final MembersRegistry membersRegistry;

    private final Configuration configuration;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    @Override
    public MembersRegistry getActiveMembers() {
        return membersRegistry;
    }

    @Inject
    public MemberConnectorImpl(final Configuration configuration, final MutableMembersRegistry members) {
        this.membersRegistry = members;
        this.configuration = configuration;
        clientBootstrap = new Bootstrap();
        workerGroup = configuration.getMemberConnectorEventLoopThreadPoolSize() > 0 ? new NioEventLoopGroup(configuration.getMemberConnectorEventLoopThreadPoolSize())
                : new NioEventLoopGroup();
        clientBootstrap.group(workerGroup);
        clientBootstrap.channel(NioSocketChannel.class);
        clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new MemberRegistryHandler(
                        members,
                        MemberConnectorImpl.this,
                        (member, channel) -> MemberConnectorImpl.this.memberActivated(member, channel),
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
    public void register(final MemberId member, final MemberConnectorListener listener) {
        stateLock.writeLock().lock();
        try {
            registered.putIfAbsent(member, listener == null ? MemberConnectorListener.getDefaultInstance() : listener);
            immutableMemberIdSet = ImmutableSet.copyOf(registered.keySet());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void unregister(MemberId member) {
        stateLock.writeLock().lock();
        try {
            registered.remove(member);
            Channel channel = registeredChannels.get(member);
            if (channel != null && channel.isActive()) {
                channel.disconnect();
            }
            immutableMemberIdSet = ImmutableSet.copyOf(registered.keySet());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Set<MemberId> getRegisteredMemberIds() {
        stateLock.readLock().lock();
        try {
            return immutableMemberIdSet;
        } finally {
            stateLock.readLock().unlock();
        }
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
        channelFuture.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                logger.warn("Connection to {} failed. Retrying in {} ms.", memberId,
                        configuration.getAutoReconnectRetryInterval());
                memberScheduledReconnect(memberId);

                channelFuture.channel().eventLoop().schedule(() -> connect(memberId),
                        configuration.getAutoReconnectRetryInterval(), TimeUnit.MILLISECONDS);
            }
        });
    }

    private void memberActivated(ActiveMember member, Channel channel) {
        MemberConnectorListener listener = registered.get(member.getMemberId());
        registeredChannels.put(member.getMemberId(), channel);
        if (listener != null) {
            listener.connected(member);
        }
    }

    private void memberScheduledReconnect(MemberId memberId) {
        MemberConnectorListener listener = registered.get(memberId);
        registeredChannels.remove(memberId);
        if (listener != null) {
            listener.scheduledReconnect(memberId);
        }
    }

    private void memberDeactivated(MemberId memberId) {
        MemberConnectorListener listener = registered.get(memberId);
        registeredChannels.remove(memberId);
        if (listener != null) {
            listener.disconnected(memberId);
        }
    }

    private void memberExceptionCaught(MemberId memberId, Throwable cause) {
        MemberConnectorListener listener = registered.get(memberId);
        registeredChannels.remove(memberId);
        if (listener != null) {
            listener.exceptionCaught(memberId, cause);
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

    @Override
    public void connectAllRegistered() {
        getRegisteredMemberIds().forEach(this::connect);
    }

}

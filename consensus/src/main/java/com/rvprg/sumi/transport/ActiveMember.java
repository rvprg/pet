package com.rvprg.sumi.transport;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public class ActiveMember {
    private final MemberId memberId;
    private final Channel channel;

    @Override
    public String toString() {
        return memberId.toString();
    }

    public MemberId getMemberId() {
        return memberId;
    }

    public Channel getChannel() {
        return channel;
    }

    public ActiveMember(Channel channel) {
        this.channel = channel;
        this.memberId = new MemberId((InetSocketAddress) channel.remoteAddress());
    }

    public ActiveMember(MemberId memberId, Channel channel) {
        this.channel = channel;
        this.memberId = memberId;
    }
}

package com.rvprg.raft.transport;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public class Member {
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

    public Member(Channel channel) {
        this.channel = channel;
        this.memberId = new MemberId((InetSocketAddress) channel.remoteAddress());
    }

    public Member(MemberId memberId, Channel channel) {
        this.channel = channel;
        this.memberId = memberId;
    }
}

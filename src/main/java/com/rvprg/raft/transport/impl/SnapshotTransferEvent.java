package com.rvprg.raft.transport.impl;

import com.rvprg.raft.sm.SnapshotDescriptor;
import com.rvprg.raft.transport.MemberId;

import io.netty.channel.Channel;

public abstract class SnapshotTransferEvent {
    private final MemberId memberId;
    private final Channel channel;
    private final SnapshotDescriptor descriptor;

    public MemberId getMemberId() {
        return memberId;
    }

    public Channel getChannel() {
        return channel;
    }

    public SnapshotDescriptor getDescriptor() {
        return descriptor;
    }

    public SnapshotTransferEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        this.memberId = memberId;
        this.channel = channel;
        this.descriptor = descriptor;
    }
}

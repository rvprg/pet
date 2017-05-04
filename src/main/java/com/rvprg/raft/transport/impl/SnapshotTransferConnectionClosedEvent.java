package com.rvprg.raft.transport.impl;

import com.rvprg.raft.transport.MemberId;

import io.netty.channel.Channel;

public class SnapshotTransferConnectionClosedEvent extends SnapshotTransferEvent {

    public SnapshotTransferConnectionClosedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

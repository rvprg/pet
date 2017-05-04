package com.rvprg.raft.transport.impl;

import com.rvprg.raft.transport.MemberId;

import io.netty.channel.Channel;

public class SnapshotTransferConnectionOpenEvent extends SnapshotTransferEvent {

    public SnapshotTransferConnectionOpenEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

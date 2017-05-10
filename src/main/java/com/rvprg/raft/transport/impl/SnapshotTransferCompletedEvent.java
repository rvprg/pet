package com.rvprg.raft.transport.impl;

import com.rvprg.raft.sm.SnapshotDescriptor;
import com.rvprg.raft.transport.MemberId;

import io.netty.channel.Channel;

public class SnapshotTransferCompletedEvent extends SnapshotTransferEvent {

    public SnapshotTransferCompletedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

package com.rvprg.raft.transport;

import com.rvprg.raft.sm.SnapshotDescriptor;

import io.netty.channel.Channel;

public class SnapshotTransferConnectionOpenEvent extends SnapshotTransferEvent {

    public SnapshotTransferConnectionOpenEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

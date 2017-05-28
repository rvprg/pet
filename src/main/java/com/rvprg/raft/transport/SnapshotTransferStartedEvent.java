package com.rvprg.raft.transport;

import com.rvprg.raft.sm.SnapshotDescriptor;

import io.netty.channel.Channel;

public class SnapshotTransferStartedEvent extends SnapshotTransferEvent {

    public SnapshotTransferStartedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

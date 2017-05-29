package com.rvprg.raft.transport;

import io.netty.channel.Channel;

public class SnapshotTransferCompletedEvent extends SnapshotTransferEvent {

    public SnapshotTransferCompletedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

package com.rvprg.raft.transport;

import io.netty.channel.Channel;

public class SnapshotTransferStartedEvent extends SnapshotTransferEvent {

    public SnapshotTransferStartedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

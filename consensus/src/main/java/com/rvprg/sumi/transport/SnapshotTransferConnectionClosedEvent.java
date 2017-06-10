package com.rvprg.sumi.transport;

import io.netty.channel.Channel;

public class SnapshotTransferConnectionClosedEvent extends SnapshotTransferEvent {

    public SnapshotTransferConnectionClosedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

package com.rvprg.sumi.transport;

import io.netty.channel.Channel;

public class SnapshotTransferConnectionOpenedEvent extends SnapshotTransferEvent {

    public SnapshotTransferConnectionOpenedEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor) {
        super(memberId, channel, descriptor);
    }

}

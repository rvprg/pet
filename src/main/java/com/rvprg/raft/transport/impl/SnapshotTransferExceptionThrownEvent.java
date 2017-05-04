package com.rvprg.raft.transport.impl;

import com.rvprg.raft.transport.MemberId;

import io.netty.channel.Channel;

public class SnapshotTransferExceptionThrownEvent extends SnapshotTransferEvent {

    private final Throwable throwable;

    public Throwable getThrowable() {
        return throwable;
    }

    public SnapshotTransferExceptionThrownEvent(MemberId memberId, Channel channel, SnapshotDescriptor descriptor, Throwable throwable) {
        super(memberId, channel, descriptor);
        this.throwable = throwable;
    }

}

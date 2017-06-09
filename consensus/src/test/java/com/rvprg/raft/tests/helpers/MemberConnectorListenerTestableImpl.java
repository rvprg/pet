package com.rvprg.raft.tests.helpers;

import java.util.concurrent.SynchronousQueue;

import com.rvprg.raft.protocol.MessageConsumer;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnectorListenerImpl;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.ChannelPipelineInitializer;

public class MemberConnectorListenerTestableImpl extends MemberConnectorListenerImpl {
    public MemberConnectorListenerTestableImpl(MessageConsumer messageConsumer, ChannelPipelineInitializer pipelineInitializer) {
        super(messageConsumer, pipelineInitializer);
    }

    private final SynchronousQueue<Boolean> connectSyncQueue = new SynchronousQueue<>();
    private final SynchronousQueue<Boolean> disconnectSyncQueue = new SynchronousQueue<>();
    private final SynchronousQueue<Boolean> reconnectSyncQueue = new SynchronousQueue<>();

    private void clearBlocking(SynchronousQueue<Boolean> b) {
        try {
            b.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void register(SynchronousQueue<Boolean> b) {
        try {
            b.put(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void awaitForConnectEvent() {
        clearBlocking(connectSyncQueue);
    }

    public void awaitForDisconnectEvent() {
        clearBlocking(disconnectSyncQueue);
    }

    public void awaitForReconnectEvent() {
        clearBlocking(reconnectSyncQueue);
    }

    @Override
    public void connected(Member member) {
        super.connected(member);
        register(connectSyncQueue);
    }

    @Override
    public void scheduledReconnect(MemberId memberId) {
        super.scheduledReconnect(memberId);
        register(reconnectSyncQueue);
    }

    @Override
    public void disconnected(MemberId memberId) {
        super.disconnected(memberId);
        register(disconnectSyncQueue);
    }

    @Override
    public void exceptionCaught(MemberId memberId, Throwable cause) {
    }

}

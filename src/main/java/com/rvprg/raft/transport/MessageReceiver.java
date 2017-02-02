package com.rvprg.raft.transport;

import com.rvprg.raft.protocol.MessageConsumer;

public interface MessageReceiver {
    public void shutdown();

    public void start(MessageConsumer messageConsumer) throws InterruptedException;
}

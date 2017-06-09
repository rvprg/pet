package com.rvprg.raft.transport;

import com.rvprg.raft.protocol.MessageConsumer;

public interface MessageReceiver extends MemberIdentifiable {
    ChannelPipelineInitializer getChannelPipelineInitializer();

    boolean isStarted();

    void shutdown() throws InterruptedException;

    void start(MessageConsumer messageConsumer) throws InterruptedException;
}

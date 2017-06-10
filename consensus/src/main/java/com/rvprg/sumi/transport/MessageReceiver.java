package com.rvprg.sumi.transport;

import com.rvprg.sumi.protocol.MessageConsumer;

public interface MessageReceiver extends MemberIdentifiable {
    ChannelPipelineInitializer getChannelPipelineInitializer();

    boolean isStarted();

    void shutdown() throws InterruptedException;

    void start(MessageConsumer messageConsumer) throws InterruptedException;
}

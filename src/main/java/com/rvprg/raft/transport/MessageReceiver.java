package com.rvprg.raft.transport;

import com.rvprg.raft.protocol.MessageConsumer;

public interface MessageReceiver extends Identifiable {
    public ChannelPipelineInitializer getChannelPipelineInitializer();

    public void shutdown();

    public void start(MessageConsumer messageConsumer) throws InterruptedException;
}

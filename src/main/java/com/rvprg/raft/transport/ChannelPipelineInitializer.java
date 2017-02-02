package com.rvprg.raft.transport;

import io.netty.channel.ChannelPipeline;

public interface ChannelPipelineInitializer {
    ChannelPipeline initialize(ChannelPipeline pipeline);
}

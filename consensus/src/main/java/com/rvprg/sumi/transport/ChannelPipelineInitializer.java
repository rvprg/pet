package com.rvprg.sumi.transport;

import java.util.List;

import io.netty.channel.ChannelPipeline;

public interface ChannelPipelineInitializer {
    ChannelPipeline initialize(ChannelPipeline pipeline);

    List<String> getHandlerNames();
}

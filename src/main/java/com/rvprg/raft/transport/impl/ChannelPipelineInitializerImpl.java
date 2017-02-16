package com.rvprg.raft.transport.impl;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RaftMessage;
import com.rvprg.raft.transport.ChannelPipelineInitializer;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import net.jcip.annotations.Immutable;

@Immutable
public class ChannelPipelineInitializerImpl implements ChannelPipelineInitializer {
    public static final String ProtobufVarint32FrameDecoder = "ProtobufVarint32FrameDecoder";
    public static final String ProtobufVarint32LengthFieldPrepender = "ProtobufVarint32LengthFieldPrepender";
    public static final String ProtobufEncoder = "ProtobufEncoder";
    public static final String ProtobufDecoderRaftMessage = "ProtobufDecoder[RaftMessage]";

    private static final List<String> handlerNames = initHandlerNames();

    private static List<String> initHandlerNames() {
        List<String> names = new ArrayList<String>();
        names.add(ProtobufVarint32FrameDecoder);
        names.add(ProtobufVarint32LengthFieldPrepender);
        names.add(ProtobufEncoder);
        names.add(ProtobufDecoderRaftMessage);
        return ImmutableList.copyOf(names);
    }

    @Override
    public ChannelPipeline initialize(ChannelPipeline pipeline) {
        return pipeline
                .addLast(ProtobufVarint32FrameDecoder, new ProtobufVarint32FrameDecoder())
                .addLast(ProtobufVarint32LengthFieldPrepender, new ProtobufVarint32LengthFieldPrepender())
                .addLast(ProtobufEncoder, new ProtobufEncoder())
                .addLast(ProtobufDecoderRaftMessage, new ProtobufDecoder(RaftMessage.getDefaultInstance()));
    }

    @Override
    public List<String> getHandlerNames() {
        return handlerNames;
    }

}

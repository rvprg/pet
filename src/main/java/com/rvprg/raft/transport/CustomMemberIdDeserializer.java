package com.rvprg.raft.transport;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class CustomMemberIdDeserializer extends JsonDeserializer<MemberId> {

    @Override
    public MemberId deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        return MemberId.fromString(jp.readValueAs(String.class));
    }
}

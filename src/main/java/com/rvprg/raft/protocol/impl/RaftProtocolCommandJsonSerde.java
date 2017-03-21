package com.rvprg.raft.protocol.impl;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class RaftProtocolCommandJsonSerde {
    private final ObjectMapper objectMapper;

    public ObjectMapper getMapper() {
        return objectMapper;
    }

    public RaftProtocolCommandJsonSerde() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        objectMapper = mapper;
    }

    public String serialize(RaftProtocolCommand obj) throws JsonProcessingException {
        return objectMapper.writeValueAsString(obj);
    }

    public RaftProtocolCommand deserialize(String json) throws JsonParseException, JsonMappingException, IOException {
        return objectMapper.readValue(json, RaftProtocolCommand.class);
    }

}

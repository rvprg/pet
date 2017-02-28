package com.rvprg.raft.sm.impl;

import com.rvprg.raft.sm.CommandDeserializer;
import com.rvprg.raft.sm.CommandSerde;
import com.rvprg.raft.sm.CommandSerializer;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CommandSerdeImpl implements CommandSerde {

    @Override
    public CommandSerializer getSerializer() {
        // TODO:
        return null;
    }

    @Override
    public CommandDeserializer getDeserializer() {
        // TODO:
        return null;
    }

}

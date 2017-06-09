package com.rvprg.raft.sm;

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

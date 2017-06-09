package com.rvprg.raft.sm;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public interface CommandDeserializer {
    Command deserialize(byte[] command) throws CommandDeserializerException;
}

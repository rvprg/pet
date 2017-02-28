package com.rvprg.raft.sm;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public interface CommandSerializer {
    byte[] marshall(Command command) throws CommandSerializerException;
}

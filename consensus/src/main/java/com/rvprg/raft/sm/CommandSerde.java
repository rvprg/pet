package com.rvprg.raft.sm;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public interface CommandSerde {
    CommandSerializer getSerializer();

    CommandDeserializer getDeserializer();
}

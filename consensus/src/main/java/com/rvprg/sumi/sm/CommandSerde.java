package com.rvprg.sumi.sm;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public interface CommandSerde {
    CommandSerializer getSerializer();

    CommandDeserializer getDeserializer();
}

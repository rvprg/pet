package com.rvprg.sumi.sm;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public interface CommandSerializer {
    byte[] marshall(Command command) throws CommandSerializerException;
}

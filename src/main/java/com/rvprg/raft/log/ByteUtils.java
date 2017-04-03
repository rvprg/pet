package com.rvprg.raft.log;

import java.nio.ByteBuffer;

public class ByteUtils {
    public static byte[] toBytes(long value) {
        ByteBuffer bbArr = ByteBuffer.allocate(8);
        bbArr.putLong(value);
        return bbArr.array();
    }

    public static long fromBytes(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }
}

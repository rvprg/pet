package com.rvprg.raft.log;

import java.nio.ByteBuffer;

public class ByteUtils {
    public static byte[] toBytes(int value) {
        ByteBuffer bbArr = ByteBuffer.allocate(4);
        bbArr.putInt(value);
        return bbArr.array();
    }

    public static int fromBytes(byte[] value) {
        return ByteBuffer.wrap(value).getInt();
    }
}

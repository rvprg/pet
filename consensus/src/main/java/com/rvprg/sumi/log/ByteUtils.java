package com.rvprg.sumi.log;

import java.nio.ByteBuffer;

public class ByteUtils {
    public static byte[] longToBytes(long value) {
        ByteBuffer bbArr = ByteBuffer.allocate(8);
        bbArr.putLong(value);
        return bbArr.array();
    }

    public static long longFromBytes(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }

    public static byte[] intToBytes(int value) {
        ByteBuffer bbArr = ByteBuffer.allocate(4);
        bbArr.putInt(value);
        return bbArr.array();
    }

    public static int intFromBytes(byte[] value) {
        return ByteBuffer.wrap(value).getInt();
    }
}

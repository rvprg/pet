package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.rvprg.raft.log.ByteUtils;

public class ByteUtilsTest {
    @Test
    public void testLongToBytes() {
        long origValue = 0xFAFAFAFAFAFAFAFAL;
        byte[] bytes = ByteUtils.longToBytes(origValue);
        long longValue = ByteUtils.longFromBytes(bytes);
        assertEquals(origValue, longValue);
    }

    @Test
    public void testIntToBytes() {
        int origValue = 0xFAFAFAFA;
        byte[] bytes = ByteUtils.intToBytes(origValue);
        int intValue = ByteUtils.intFromBytes(bytes);
        assertEquals(origValue, intValue);
    }

}

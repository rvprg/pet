package com.rvprg.raft.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.rvprg.raft.protocol.impl.LogEntry;

public class LogEntryTest {
    @Test
    public void testEquals() {
        LogEntry e1 = new LogEntry(1, new byte[] { 1, 2, 3 });
        LogEntry e2 = new LogEntry(1, new byte[] { 1, 2, 3 });

        assertTrue(e1.equals(e2));
        assertTrue(e2.equals(e1));
    }

    @Test
    public void testNotEquals_Terms() {
        LogEntry e1 = new LogEntry(1, new byte[] { 1, 2, 3 });
        LogEntry e2 = new LogEntry(2, new byte[] { 1, 2, 3 });

        assertFalse(e1.equals(e2));
        assertFalse(e2.equals(e1));
    }

    @Test
    public void testNotEquals_Command() {
        LogEntry e1 = new LogEntry(1, new byte[] { 1, 2, 3 });
        LogEntry e2 = new LogEntry(1, new byte[] { 3, 2, 1 });

        assertFalse(e1.equals(e2));
        assertFalse(e2.equals(e1));
    }
}

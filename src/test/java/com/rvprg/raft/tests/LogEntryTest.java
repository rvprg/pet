package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.rvprg.raft.log.LogEntryFactory;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;

public class LogEntryTest {

    @Test
    public void testEquals() {
        LogEntry e1 = LogEntryFactory.create(1, new byte[] { 1, 2, 3 });
        LogEntry e2 = LogEntryFactory.create(1, new byte[] { 1, 2, 3 });

        assertTrue(e1.equals(e2));
        assertTrue(e2.equals(e1));
    }

    @Test
    public void testEquals_NoOperation() {
        LogEntry e1 = LogEntryFactory.create(1);
        LogEntry e2 = LogEntryFactory.create(1);

        assertTrue(e1.equals(e2));
        assertTrue(e2.equals(e1));

        assertEquals(e1.getType(), LogEntryType.NoOperationCommand);
        assertEquals(e2.getType(), LogEntryType.NoOperationCommand);
    }

    @Test
    public void testEquals_Type() {
        LogEntry e1 = LogEntryFactory.create(1, LogEntryType.RaftProtocolCommand, new byte[] { 1, 2, 3 });
        LogEntry e2 = LogEntryFactory.create(1, LogEntryType.RaftProtocolCommand, new byte[] { 1, 2, 3 });

        assertTrue(e1.equals(e2));
        assertTrue(e2.equals(e1));
    }

    @Test
    public void testNotEquals_Terms() {
        LogEntry e1 = LogEntryFactory.create(1, new byte[] { 1, 2, 3 });
        LogEntry e2 = LogEntryFactory.create(2, new byte[] { 1, 2, 3 });

        assertFalse(e1.equals(e2));
        assertFalse(e2.equals(e1));
    }

    @Test
    public void testNotEquals_Type() {
        LogEntry e1 = LogEntryFactory.create(1, LogEntryType.RaftProtocolCommand, new byte[] { 1, 2, 3 });
        LogEntry e2 = LogEntryFactory.create(1, LogEntryType.StateMachineCommand, new byte[] { 1, 2, 3 });

        assertFalse(e1.equals(e2));
        assertFalse(e2.equals(e1));
    }

    @Test
    public void testNotEquals_Command() {
        LogEntry e1 = LogEntryFactory.create(1, new byte[] { 1, 2, 3 });
        LogEntry e2 = LogEntryFactory.create(1, new byte[] { 3, 2, 1 });

        assertFalse(e1.equals(e2));
        assertFalse(e2.equals(e1));
    }
}

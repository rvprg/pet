package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rvprg.raft.protocol.impl.LogEntry;
import com.rvprg.raft.protocol.impl.TransientLogImpl;

public class TransientLogTest {
    private TransientLogImpl log;

    @Before
    public void init() {
        log = new TransientLogImpl();
    }

    @After
    public void close() throws IOException {
        log.close();
    }

    @Test
    public void testSimpleAppendAndGet() throws IOException {
        LogEntry logEntry1 = new LogEntry(1, ByteBuffer.allocate(0));
        LogEntry logEntry2 = new LogEntry(1, ByteBuffer.allocate(0));
        LogEntry logEntry3 = new LogEntry(2, ByteBuffer.allocate(0));

        int index1 = log.append(logEntry1);
        int index2 = log.append(logEntry2);
        int index3 = log.append(logEntry3);

        assertEquals(0, index1);
        assertEquals(1, index2);
        assertEquals(2, index3);
        assertEquals(2, log.getLastIndex());

        assertEquals(logEntry1, log.get(0));
        assertEquals(logEntry2, log.get(1));
        assertEquals(logEntry3, log.get(2));

        assertNull(log.get(3));

        List<LogEntry> l1 = log.get(1, 1);
        assertEquals(1, l1.size());
        assertEquals(logEntry2, l1.get(0));

        List<LogEntry> l2 = log.get(1, 3);
        assertEquals(2, l2.size());
        assertEquals(logEntry2, l2.get(0));
        assertEquals(logEntry3, l2.get(1));
    }

    @Test
    public void testComplexAppend_ShouldSucceed1() throws IOException {
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 0
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 1
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 2
        log.append(new LogEntry(2, ByteBuffer.allocate(0))); // 3
        log.append(new LogEntry(3, ByteBuffer.allocate(0))); // 4

        assertEquals(4, log.getLastIndex());

        LogEntry logEntry1 = new LogEntry(2, ByteBuffer.allocate(0));
        LogEntry logEntry2 = new LogEntry(2, ByteBuffer.allocate(0));

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);

        boolean res = log.append(3, 2, logEntries);
        assertEquals(true, res);
        assertEquals(5, log.getLastIndex());

        assertEquals(logEntry1, log.get(4));
        assertEquals(logEntry2, log.get(5));
    }

    @Test
    public void testComplexAppend_ShouldSucceed2() throws IOException {
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 0
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 1
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 2
        log.append(new LogEntry(2, ByteBuffer.allocate(0))); // 3
        log.append(new LogEntry(3, ByteBuffer.allocate(0))); // 4

        assertEquals(4, log.getLastIndex());

        LogEntry logEntry1 = new LogEntry(2, ByteBuffer.allocate(0));
        LogEntry logEntry2 = new LogEntry(2, ByteBuffer.allocate(0));

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);

        boolean res = log.append(4, 3, logEntries);
        assertEquals(true, res);
        assertEquals(6, log.getLastIndex());
    }

    @Test
    public void testComplexAppend_ShouldFail1() throws IOException {
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 0
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 1
        log.append(new LogEntry(1, ByteBuffer.allocate(0))); // 2
        log.append(new LogEntry(2, ByteBuffer.allocate(0))); // 3
        log.append(new LogEntry(3, ByteBuffer.allocate(0))); // 4

        assertEquals(4, log.getLastIndex());

        LogEntry logEntry1 = new LogEntry(2, ByteBuffer.allocate(0));
        LogEntry logEntry2 = new LogEntry(2, ByteBuffer.allocate(0));

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);

        boolean res = log.append(3, 1, logEntries);
        assertEquals(false, res);
        assertEquals(4, log.getLastIndex());
    }

    @Test
    public void testComplexAppend_ShouldFail2() throws IOException {
        assertEquals(false, log.append(3, 1, null));
        assertEquals(false, log.append(3, 1, new ArrayList<LogEntry>()));
        assertEquals(false, log.append(-1, 1, new ArrayList<LogEntry>()));
    }
}

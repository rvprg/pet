package com.rvprg.raft.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.impl.LogEntry;
import com.rvprg.raft.protocol.impl.TransientLogImpl;
import com.rvprg.raft.sm.StateMachine;

@RunWith(Parameterized.class)
public class LogTest {
    private Log log;

    public LogTest(Log log) {
        this.log = log;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<? extends Object> data() {
        return Arrays.asList(new TransientLogImpl());
    }

    @Before
    public void init() {
        log.init();
    }

    @After
    public void close() throws IOException {
        log.close();
    }

    @Test
    public void testSimpleAppendAndGet() throws IOException {
        LogEntry logEntry1 = new LogEntry(1, new byte[0]);
        LogEntry logEntry2 = new LogEntry(1, new byte[0]);
        LogEntry logEntry3 = new LogEntry(2, new byte[0]);

        int index1 = log.append(logEntry1);
        int index2 = log.append(logEntry2);
        int index3 = log.append(logEntry3);

        assertEquals(1, index1);
        assertEquals(2, index2);
        assertEquals(3, index3);

        assertEquals(3, log.getLastIndex());

        assertEquals(logEntry1, log.get(1));
        assertEquals(logEntry2, log.get(2));
        assertEquals(logEntry3, log.get(3));

        assertNull(log.get(4));

        List<LogEntry> l1 = log.get(1, 1);
        assertEquals(1, l1.size());
        assertEquals(logEntry1, l1.get(0));

        List<LogEntry> l2 = log.get(1, 3);
        assertEquals(3, l2.size());
        assertEquals(logEntry1, l2.get(0));
        assertEquals(logEntry2, l2.get(1));
        assertEquals(logEntry3, l2.get(2));

        List<LogEntry> l3 = log.get(1, 10);
        assertEquals(3, l3.size());
        assertEquals(logEntry1, l2.get(0));
        assertEquals(logEntry2, l2.get(1));
        assertEquals(logEntry3, l2.get(2));

        List<LogEntry> l4 = log.get(1, -1);
        assertEquals(0, l4.size());

        List<LogEntry> l5 = log.get(-1, -1);
        assertEquals(0, l5.size());
    }

    @Test
    public void testComplexAppend_ShouldSucceed1() throws IOException {
        log.append(new LogEntry(1, new byte[0])); // 1
        log.append(new LogEntry(1, new byte[0])); // 2
        log.append(new LogEntry(1, new byte[0])); // 3
        log.append(new LogEntry(2, new byte[0])); // 4
        log.append(new LogEntry(3, new byte[0])); // 5

        assertEquals(5, log.getLastIndex());

        LogEntry logEntry1 = new LogEntry(2, new byte[0]);
        LogEntry logEntry2 = new LogEntry(2, new byte[0]);

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);

        boolean res = log.append(4, 2, logEntries);
        assertEquals(true, res);
        assertEquals(6, log.getLastIndex());

        assertEquals(logEntry1, log.get(5));
        assertEquals(logEntry2, log.get(6));
    }

    @Test
    public void testComplexAppend_ShouldSucceed2() throws IOException {
        log.append(new LogEntry(1, new byte[0])); // 1
        log.append(new LogEntry(1, new byte[0])); // 2
        log.append(new LogEntry(1, new byte[0])); // 3
        log.append(new LogEntry(2, new byte[0])); // 4
        log.append(new LogEntry(3, new byte[0])); // 5

        assertEquals(5, log.getLastIndex());

        LogEntry logEntry1 = new LogEntry(2, new byte[0]);
        LogEntry logEntry2 = new LogEntry(2, new byte[0]);

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);

        boolean res = log.append(5, 3, logEntries);
        assertEquals(true, res);
        assertEquals(7, log.getLastIndex());
    }

    @Test
    public void testComplexAppend_ShouldSucceed3() throws IOException {
        LogEntry[] logEntriesArr = new LogEntry[] {
                new LogEntry(1, new byte[0]), // 1
                new LogEntry(1, new byte[0]), // 2
                new LogEntry(1, new byte[0]), // 3
                new LogEntry(2, new byte[0]), // 4
                new LogEntry(3, new byte[0]) // 5
        };

        for (LogEntry le : logEntriesArr) {
            log.append(le);
        }

        assertEquals(5, log.getLastIndex());

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntriesArr[3]);
        logEntries.add(logEntriesArr[4]);

        boolean res = log.append(3, 1, logEntries);
        assertEquals(true, res);

        assertEquals(5, log.getLastIndex());

        for (int i = 1; i < logEntriesArr.length; ++i) {
            assertEquals(logEntriesArr[i - 1], log.get(i));
        }
    }

    @Test
    public void testComplexAppend_ShouldFail1() throws IOException {
        log.append(new LogEntry(1, new byte[0])); // 1
        log.append(new LogEntry(1, new byte[0])); // 2
        log.append(new LogEntry(1, new byte[0])); // 3
        log.append(new LogEntry(2, new byte[0])); // 4
        log.append(new LogEntry(3, new byte[0])); // 5

        assertEquals(5, log.getLastIndex());

        LogEntry logEntry1 = new LogEntry(2, new byte[0]);
        LogEntry logEntry2 = new LogEntry(2, new byte[0]);

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);

        boolean res = log.append(4, 1, logEntries);
        assertEquals(false, res);
        assertEquals(5, log.getLastIndex());
    }

    @Test
    public void testComplexAppend_ShouldFail2() throws IOException {
        assertEquals(false, log.append(3, 1, null));

        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(new LogEntry(2, new byte[0]));
        logEntries.add(new LogEntry(2, new byte[0]));

        assertEquals(false, log.append(3, 1, logEntries));
        assertEquals(false, log.append(-1, 1, logEntries));
    }

    @Test
    public void testCommit() {
        LogEntry[] logEntries = new LogEntry[] {
                new LogEntry(1, new byte[0]),
                new LogEntry(1, new byte[] { 1 }),
                new LogEntry(1, new byte[] { 2 }),
                new LogEntry(1, new byte[0]),
                new LogEntry(1, new byte[] { 3 })
        };

        for (LogEntry le : logEntries) {
            log.append(le);
        }

        final List<byte[]> commands = new ArrayList<>();
        StateMachine stateMachine = new StateMachine() {
            @Override
            public void apply(byte[] command) {
                commands.add(command);
            }
        };

        assertEquals(1, log.getCommitIndex());

        log.commit(3, stateMachine);

        assertEquals(3, log.getCommitIndex());
        assertEquals(2, commands.size());

        assertArrayEquals(logEntries[1].getCommand(), commands.get(0));
        assertArrayEquals(logEntries[2].getCommand(), commands.get(1));

        log.commit(3, stateMachine);

        assertEquals(3, log.getCommitIndex());
        assertEquals(2, commands.size());

        assertArrayEquals(logEntries[1].getCommand(), commands.get(0));
        assertArrayEquals(logEntries[2].getCommand(), commands.get(1));

        log.commit(10, stateMachine);

        assertEquals(5, log.getCommitIndex());
        assertEquals(3, commands.size());

        assertArrayEquals(logEntries[1].getCommand(), commands.get(0));
        assertArrayEquals(logEntries[2].getCommand(), commands.get(1));
        assertArrayEquals(logEntries[4].getCommand(), commands.get(2));
    }
}

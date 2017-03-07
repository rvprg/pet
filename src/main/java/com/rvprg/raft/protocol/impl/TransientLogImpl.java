package com.rvprg.raft.protocol.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.rvprg.raft.protocol.Log;

public class TransientLogImpl implements Log {
    private final ArrayList<LogEntry> log = new ArrayList<LogEntry>();
    private final AtomicInteger commitIndex = new AtomicInteger();
    private final AtomicInteger lastApplied = new AtomicInteger();

    public TransientLogImpl() {
        log.add(new LogEntry(0, ByteBuffer.allocate(0)));
    }

    @Override
    public synchronized void close() throws IOException {
        // nop
    }

    @Override
    public synchronized int getCommitIndex() {
        return commitIndex.get();
    }

    @Override
    public int incrementAndGetCommitIndex() {
        return commitIndex.incrementAndGet();
    }

    @Override
    public int getLastApplied() {
        return lastApplied.get();
    }

    @Override
    public int incrementAndGetLastApplied() {
        return lastApplied.incrementAndGet();
    }

    @Override
    public synchronized boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries) {
        if (logEntries == null || logEntries.isEmpty()) {
            return false;
        }

        LogEntry prevEntry = get(prevLogIndex);
        if (prevEntry == null) {
            return false;
        }

        if (prevEntry.getTerm() != prevLogTerm) {
            return false;
        }

        LogEntry newNextEntry = logEntries.get(0);

        int nextEntryIndex = prevLogIndex + 1;
        LogEntry nextEntry = get(nextEntryIndex);
        if (nextEntry != null && nextEntry.getTerm() != newNextEntry.getTerm()) {
            List<LogEntry> newLog = new ArrayList<LogEntry>(log.subList(0, nextEntryIndex));
            log.clear();
            log.addAll(newLog);
        }

        for (int i = nextEntryIndex, j = 0; j < logEntries.size(); ++i, ++j) {
            insert(i, logEntries.get(j));
        }

        return true;
    }

    private void insert(int index, LogEntry logEntry) {
        if (index > log.size() - 1) {
            log.add(logEntry);
        } else {
            log.set(index, logEntry);
        }
    }

    @Override
    public synchronized LogEntry get(int index) {
        if (index >= log.size() || index < 0) {
            return null;
        }
        return log.get(index);
    }

    @Override
    public synchronized LogEntry getLast() {
        return log.get(log.size() - 1);
    }

    @Override
    public synchronized int getLastIndex() {
        return log.size() - 1;
    }

    @Override
    public synchronized List<LogEntry> get(int nextIndex, int maxNum) {
        if (nextIndex > log.size() - 1 || nextIndex < 0 || maxNum <= 0) {
            return new ArrayList<LogEntry>();
        }

        if (nextIndex + maxNum > log.size() - 1) {
            return log.subList(nextIndex, log.size());
        }

        return log.subList(nextIndex, nextIndex + maxNum);
    }

    @Override
    public int append(LogEntry logEntry) {
        log.add(logEntry);
        return log.size() - 1;
    }

    @Override
    public synchronized void clear() {
        log.clear();
        commitIndex.set(0);
        lastApplied.set(0);
    }

    @Override
    public String toString() {
        return "Transient log implementation";
    }

}

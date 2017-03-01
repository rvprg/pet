package com.rvprg.raft.protocol.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rvprg.raft.protocol.Log;

public class TransientLogImpl implements Log {

    private final ArrayList<LogEntry> log = new ArrayList<LogEntry>();

    @Override
    public synchronized void close() throws IOException {
    }

    @Override
    public synchronized int getCommitIndex() {
        return 0;
    }

    @Override
    public synchronized boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries) {
        return true;
    }

    @Override
    public LogEntry get(int index) {
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
    public synchronized int getLastApplied() {
        return 0;
    }

    @Override
    public synchronized int append(LogEntry logEntry) {
        log.add(logEntry);
        return log.size() - 1;
    }

}

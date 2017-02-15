package com.rvprg.raft.protocol.impl;

import java.io.IOException;
import java.util.ArrayList;

import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.LogEntry;

public class TransientLogImpl implements Log {

    ArrayList<LogEntry> log = new ArrayList<LogEntry>();

    @Override
    public void close() throws IOException {
    }

    @Override
    public int getCommitIndex() {
        return 0;
    }

    @Override
    public int lastApplied() {
        return 0;
    }

    @Override
    public void add(int index, LogEntry logEntry) {
        log.add(index, logEntry);
    }

    @Override
    public int length() {
        return log.size();
    }

    @Override
    public LogEntry get(int index) {
        return log.get(index);
    }

    @Override
    public LogEntry getLast() {
        return log.get(log.size() - 1);
    }

}

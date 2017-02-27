package com.rvprg.raft.protocol.impl;

import java.io.IOException;
import java.util.ArrayList;

import com.rvprg.raft.protocol.Log;

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
    public int append(LogEntry logEntry) {
        log.add(logEntry);
        return log.size() - 1;
    }

    @Override
    public LogEntry get(int index) {
        return log.get(index);
    }

    @Override
    public LogEntry getLast() {
        return log.get(log.size() - 1);
    }

    @Override
    public int getLastIndex() {
        return 0;
    }

    @Override
    public int getLastApplied() {
        return 0;
    }

}

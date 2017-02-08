package com.rvprg.raft.protocol.impl;

import java.io.IOException;

import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.LogEntry;

public class TransientLogImpl implements Log {

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

    }

}

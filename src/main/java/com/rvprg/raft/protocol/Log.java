package com.rvprg.raft.protocol;

import java.io.Closeable;

public interface Log extends Closeable {
    int getCommitIndex();

    int lastApplied();

    void add(int index, LogEntry logEntry);
}

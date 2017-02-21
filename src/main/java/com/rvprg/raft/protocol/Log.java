package com.rvprg.raft.protocol;

import java.io.Closeable;

public interface Log extends Closeable {
    int getCommitIndex();

    int getLastApplied();

    int getLastIndex();

    LogEntry getLast();

    void append(LogEntry logEntry);

    LogEntry get(int index);
}

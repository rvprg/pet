package com.rvprg.raft.protocol;

import java.io.Closeable;

import com.rvprg.raft.protocol.impl.LogEntry;

public interface Log extends Closeable {
    int getCommitIndex();

    int getLastApplied();

    int getLastIndex();

    LogEntry getLast();

    int append(LogEntry logEntry);

    LogEntry get(int index);
}

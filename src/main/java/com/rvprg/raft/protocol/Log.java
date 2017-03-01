package com.rvprg.raft.protocol;

import java.io.Closeable;
import java.util.List;

import com.rvprg.raft.protocol.impl.LogEntry;

public interface Log extends Closeable {
    int getCommitIndex();

    int getLastApplied();

    int getLastIndex();

    LogEntry getLast();

    int append(LogEntry logEntry);

    boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries);

    LogEntry get(int index);

}

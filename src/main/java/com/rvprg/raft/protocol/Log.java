package com.rvprg.raft.protocol;

import java.io.Closeable;
import java.util.List;

import com.rvprg.raft.protocol.impl.LogEntry;

public interface Log extends Closeable {
    int getCommitIndex();

    int incrementAndGetCommitIndex();

    int updateCommitIndex(int commitIndex);

    int getLastApplied();

    int incrementAndGetLastApplied();

    int getLastIndex();

    int getFirstIndex();

    LogEntry getLast();

    boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries);

    LogEntry get(int index);

    List<LogEntry> get(int nextIndex, int maxNum);

    int append(LogEntry logEntry);

    void init();

}

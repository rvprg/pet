package com.rvprg.raft.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.sm.StateMachine;

public interface Log extends Closeable {
    int getCommitIndex();

    int commit(int commitUpToIndex, StateMachine stateMachine);

    int getLastIndex();

    int getFirstIndex();

    LogEntry getLast();

    boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries);

    LogEntry get(int index);

    List<LogEntry> get(int nextIndex, int maxNum);

    int append(LogEntry logEntry);

    void init(String name) throws IOException;

}

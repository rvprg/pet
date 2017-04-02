package com.rvprg.raft.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.sm.StateMachine;

public interface Log extends Closeable {
    int getCommitIndex();

    int commit(int commitUpToIndex, StateMachine stateMachine) throws LogException;

    int getLastIndex();

    int getFirstIndex();

    LogEntry getLast() throws LogException;

    boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries) throws LogException;

    LogEntry get(int index) throws LogException;

    List<LogEntry> get(int nextIndex, int maxNum) throws LogException;

    int append(LogEntry logEntry);

    void init(String name) throws IOException;

    void delete() throws IOException;
}

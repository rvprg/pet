package com.rvprg.raft.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.transport.MemberId;

public interface Log extends Closeable {
    void setVotedFor(MemberId memberId);

    MemberId getVotedFor();

    void setTerm(int term);

    int getTerm();

    long getCommitIndex();

    long commit(long commitUpToIndex, StateMachine stateMachine) throws LogException;

    long getLastIndex();

    long getFirstIndex();

    LogEntry getLast() throws LogException;

    boolean append(long prevLogIndex, long prevLogTerm, List<LogEntry> logEntries) throws LogException;

    LogEntry get(long index) throws LogException;

    List<LogEntry> get(long nextIndex, int maxNum) throws LogException;

    long append(LogEntry logEntry);

    void init(String name) throws IOException;

    void delete() throws IOException;

    void truncate(long toIndex) throws LogException;
}

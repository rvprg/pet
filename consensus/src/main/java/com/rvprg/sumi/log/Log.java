package com.rvprg.sumi.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.transport.MemberConnector;
import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.SnapshotDescriptor;

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

    void initialize(Configuration configuration) throws IOException;

    void delete() throws IOException;

    void truncate(long toIndex) throws LogException;

    void setFakeLogEntryAndCommit(long index, int term);

    void installSnapshot(StateMachine stateMachine, MemberConnector memberConnector, SnapshotDescriptor snapshotDescriptor) throws LogException, SnapshotInstallException;

    SnapshotDescriptor getSnapshotAndTruncate(StateMachine stateMachine, MemberConnector memberConnector) throws LogException;
}

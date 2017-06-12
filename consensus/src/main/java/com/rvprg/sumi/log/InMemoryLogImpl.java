package com.rvprg.sumi.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.transport.MemberConnector;
import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.SnapshotDescriptor;

public class InMemoryLogImpl implements Log {
    private final ArrayList<LogEntry> log = new ArrayList<LogEntry>();
    private int commitIndex = 0;
    private long firstIndex = 0;
    private int term;
    private MemberId votedFor;

    private long fakeIndex;
    private int fakeTerm;

    public InMemoryLogImpl() {
        initialize(null);
    }

    @Override
    public synchronized void close() throws IOException {
        // nop
    }

    @Override
    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    @Override
    public synchronized boolean append(long prevLogIndex, long prevLogTerm, List<LogEntry> logEntries) {
        if (logEntries == null || logEntries.isEmpty()) {
            return false;
        }

        LogEntry prevEntry = get(prevLogIndex);
        if (prevEntry == null) {
            return false;
        }

        if (prevEntry.getTerm() != prevLogTerm) {
            return false;
        }

        LogEntry newNextEntry = logEntries.get(0);

        long nextEntryIndex = prevLogIndex + 1;
        LogEntry nextEntry = get(nextEntryIndex);
        if (nextEntry != null && nextEntry.getTerm() != newNextEntry.getTerm()) {
            while (log.size() > nextEntryIndex) {
                log.remove(log.size() - 1);
            }
        }

        for (long i = nextEntryIndex, j = 0; j < logEntries.size(); ++i, ++j) {
            insert((int) i, logEntries.get((int) j));
        }

        return true;
    }

    private void insert(int index, LogEntry logEntry) {
        if (index > log.size() - 1) {
            log.add(logEntry);
        } else {
            log.set(index, logEntry);
        }
    }

    @Override
    public synchronized LogEntry get(long index) {
        if (fakeIndex == index) {
            return LogEntryFactory.create(fakeTerm);
        }

        if (index >= log.size() || index < 0 || index < getFirstIndex()) {
            return null;
        }
        return log.get((int) index);
    }

    @Override
    public synchronized LogEntry getLast() {
        return get(getLastIndex());
    }

    @Override
    public synchronized long getLastIndex() {
        return log.size() - 1;
    }

    @Override
    public synchronized List<LogEntry> get(long nextIndex, int maxNum) {
        if (nextIndex > log.size() - 1 || nextIndex < 0 || maxNum <= 0) {
            return new ArrayList<LogEntry>();
        }

        if (nextIndex + maxNum > log.size() - 1) {
            return log.subList((int) nextIndex, log.size());
        }

        return log.subList((int) nextIndex, (int) (nextIndex + maxNum));
    }

    @Override
    public long append(LogEntry logEntry) {
        log.add(logEntry);
        return log.size() - 1;
    }

    @Override
    public synchronized void initialize(Configuration configuration) {
        log.clear();
        commitIndex = 1;
        firstIndex = 0;
        LogEntry logEntry = LogEntry.newBuilder().setTerm(0).setType(LogEntryType.NoOperationCommand).setEntry(ByteString.copyFrom(new byte[0])).build();
        log.add(logEntry);
    }

    @Override
    public String toString() {
        return "In-Memory log";
    }

    @Override
    public synchronized long commit(long commitUpToIndex, StateMachine stateMachine) {
        if (commitUpToIndex > getCommitIndex()) {
            long newIndex = Math.min(commitUpToIndex, getLastIndex());

            for (long i = getCommitIndex() + 1; i <= newIndex; ++i) {
                LogEntry logEntry = get(i);
                if (logEntry.getType() == LogEntryType.StateMachineCommand) {
                    stateMachine.apply(logEntry.getEntry().toByteArray());
                }
            }

            this.commitIndex = (int) newIndex;
        }
        return this.commitIndex;

    }

    @Override
    public synchronized long getFirstIndex() {
        return firstIndex;
    }

    @Override
    public void delete() {
        // nop
    }

    @Override
    public void setTerm(int term) {
        this.term = term;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public void setVotedFor(MemberId memberId) {
        this.votedFor = memberId;
    }

    @Override
    public MemberId getVotedFor() {
        return votedFor;
    }

    @Override
    public void truncate(long toIndex) throws LogException {
        if (toIndex > getCommitIndex()) {
            throw new LogException("toIndex > getCommitIndex()");
        }

        firstIndex = toIndex;
    }

    @Override
    public synchronized void setFakeLogEntryAndCommit(long index, int term) {
        this.fakeIndex = index;
        this.fakeTerm = term;
        commitIndex = (int) index;
    }

    @Override
    public void installSnapshot(StateMachine stateMachine, MemberConnector memberConnector, SnapshotDescriptor snapshotDescriptor) throws LogException, SnapshotInstallException {
        throw new LogException("Not implemented");
    }

    @Override
    public SnapshotDescriptor getSnapshotAndTruncate(StateMachine stateMachine, MemberConnector memberConnector) throws LogException {
        throw new LogException("Not implemented");
    }

}

package com.rvprg.raft.log.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;
import com.rvprg.raft.sm.StateMachine;

public class TransientLogImpl implements Log {
    private final ArrayList<LogEntry> log = new ArrayList<LogEntry>();
    private int commitIndex = 0;
    private int firstIndex = 0;

    public TransientLogImpl() {
        init("");
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
        if (index >= log.size() || index < 0) {
            return null;
        }
        return log.get((int) index);
    }

    @Override
    public synchronized LogEntry getLast() {
        return log.get((int) getLastIndex());
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
    public synchronized void init(String name) {
        log.clear();
        commitIndex = 1;
        firstIndex = 0;
        LogEntry logEntry = LogEntry.newBuilder().setTerm(0).setType(LogEntryType.NoOperationCommand).setEntry(ByteString.copyFrom(new byte[0])).build();
        log.add(logEntry);
    }

    @Override
    public String toString() {
        return "Transient log implementation";
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

}

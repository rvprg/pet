package com.rvprg.raft.log.impl;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.log.LogEntryFactory;
import com.rvprg.raft.log.LogException;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;
import com.rvprg.raft.sm.StateMachine;

public class LevelDBLogImpl implements Log {

    // TODO: Synchronizatoin is completely broken.
    private volatile DB database;
    private File databaseFile;

    private static byte[] LastIndexKey = "LastIndex".getBytes();
    private static byte[] FirstIndexKey = "FirstIndexKey".getBytes();
    private static byte[] CommitIndexKey = "CommitIndexKey".getBytes();

    @Override
    public void close() throws IOException {
        if (database != null) {
            database.close();
            database = null;
        }
    }

    private int getIntegerValueByKey(byte[] key) {
        byte[] value = database.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Key not found");
        }
        return fromBytes(value);
    }

    @Override
    public int getCommitIndex() {
        return getIntegerValueByKey(CommitIndexKey);
    }

    @Override
    public int commit(int commitUpToIndex, StateMachine stateMachine) throws LogException {
        if (commitUpToIndex > getCommitIndex()) {
            int newIndex = Math.min(commitUpToIndex, getLastIndex());

            for (int i = getCommitIndex() + 1; i <= newIndex; ++i) {
                LogEntry logEntry = get(i);
                if (logEntry.getType() == LogEntryType.StateMachineCommand) {
                    stateMachine.apply(logEntry.getEntry().toByteArray());
                }
            }

            setCommitIndex(newIndex);
        }
        return getCommitIndex();
    }

    private void setCommitIndex(int index) {
        setIntegerValueByKey(CommitIndexKey, index);
    }

    @Override
    public int getLastIndex() {
        return getIntegerValueByKey(LastIndexKey);
    }

    private void setLastIndex(int index) {
        setIntegerValueByKey(LastIndexKey, index);
    }

    private void setIntegerValueByKey(byte[] key, int index) {
        database.put(key, toBytes(index));
    }

    @Override
    public int getFirstIndex() {
        return getIntegerValueByKey(FirstIndexKey);
    }

    private void setFirstIndex(int index) {
        setIntegerValueByKey(FirstIndexKey, index);
    }

    @Override
    public LogEntry getLast() throws LogException {
        return get(getLastIndex());
    }

    @Override
    public boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries) throws LogException {
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

        int nextEntryIndex = prevLogIndex + 1;
        LogEntry nextEntry = get(nextEntryIndex);
        WriteBatch wb = database.createWriteBatch();
        try {
            if (nextEntry != null && nextEntry.getTerm() != newNextEntry.getTerm()) {
                int lastIndex = getLastIndex();
                for (int currIndex = nextEntryIndex; currIndex <= lastIndex; ++currIndex) {
                    wb.delete(toBytes(currIndex));
                }
            }

            int currIndex = nextEntryIndex;
            for (int j = 0; j < logEntries.size(); ++j) {
                wb.put(toBytes(currIndex + j), logEntries.get(j).toByteArray());
            }

            database.write(wb);
            setLastIndex(currIndex + logEntries.size() - 1);

            return true;
        } finally {
            try {
                wb.close();
            } catch (IOException e) {
                throw new LogException(e);
            }
        }
    }

    @Override
    public LogEntry get(int index) throws LogException {
        try {
            byte[] logEntry = database.get(toBytes(index));
            if (logEntry == null) {
                return null;
            }
            return LogEntry.parseFrom(logEntry);
        } catch (InvalidProtocolBufferException e) {
            throw new LogException(e);
        }
    }

    @Override
    public List<LogEntry> get(int nextIndex, int maxNum) throws LogException {
        int lastLogIndex = getLastIndex();

        ArrayList<LogEntry> retArr = new ArrayList<LogEntry>();
        if (nextIndex > lastLogIndex || nextIndex < 0 || maxNum <= 0) {
            return retArr;
        }

        int currIndex = nextIndex;
        while (currIndex < nextIndex + maxNum) {
            byte[] value = database.get(toBytes(currIndex));
            if (value == null) {
                return retArr;
            }
            try {
                retArr.add(LogEntry.parseFrom(value));
            } catch (InvalidProtocolBufferException e) {
                throw new LogException(e);
            }
            currIndex++;
        }

        return retArr;
    }

    @Override
    public int append(LogEntry logEntry) {
        int nextIndex = getLastIndex() + 1;
        database.put(toBytes(nextIndex), logEntry.toByteArray());
        setLastIndex(nextIndex);
        return nextIndex;
    }

    @Override
    public void init(String name) throws IOException {
        if (database != null) {
            throw new IllegalStateException("Already initialized");
        }

        Options options = new Options();
        options.createIfMissing(true);

        databaseFile = new File(name);
        database = factory.open(databaseFile, options);

        setLastIndex(-1);
        setFirstIndex(0);
        append(LogEntryFactory.create(0));
        setCommitIndex(1);
    }

    private byte[] toBytes(int value) {
        ByteBuffer bbArr = ByteBuffer.allocate(4);
        bbArr.putInt(value);
        return bbArr.array();
    }

    private int fromBytes(byte[] value) {
        return ByteBuffer.wrap(value).getInt();
    }

    @Override
    public void delete() throws IOException {
        if (databaseFile != null) {
            FileUtils.deleteDirectory(databaseFile);
        }
    }

    @Override
    public String toString() {
        return "LevelDB log implementation";
    }

}

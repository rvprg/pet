package com.rvprg.raft.log.impl;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.log.ByteUtils;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.log.LogEntryFactory;
import com.rvprg.raft.log.LogException;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;
import com.rvprg.raft.sm.SnapshotDescriptor;
import com.rvprg.raft.sm.SnapshotInstallException;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.sm.WritableSnapshot;
import com.rvprg.raft.transport.MemberId;

public class LevelDBLogImpl implements Log {
    private final Logger logger = LoggerFactory.getLogger(LevelDBLogImpl.class);

    private volatile DB database;
    private File databaseFile;

    private static final byte[] RaftLogKey = "RaftLog".getBytes();
    private static final byte[] LastIndexKey = "LastIndex".getBytes();
    private static final byte[] FirstIndexKey = "FirstIndexKey".getBytes();
    private static final byte[] CommitIndexKey = "CommitIndexKey".getBytes();
    private static final byte[] TermKey = "TermKey".getBytes();
    private static final byte[] VoteForKey = "VoteForKey".getBytes();

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private volatile Configuration configuration;

    private long fakeIndex = -1;
    private int fakeTerm = -1;

    @Override
    public void close() throws IOException {
        if (database != null) {
            database.close();
            database = null;
            logger.info("{} has been closed: {}.", this, configuration.getLogUri());
        }
    }

    private boolean isDatabaseInitialized() {
        return database.get(RaftLogKey) != null;
    }

    private void initializeDatabase() {
        stateLock.writeLock().lock();
        try {
            setLastIndex(-1);
            setFirstIndex(0);
            append(LogEntryFactory.create(0));
            setCommitIndex(1);
            database.put(RaftLogKey, new byte[] {});
            logger.info("{} has been initialized: {}.", this, configuration.getLogUri());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private void setCommitIndex(long index) {
        setLongValueByKey(CommitIndexKey, index);
    }

    private void setLastIndex(long index) {
        setLongValueByKey(LastIndexKey, index);
    }

    private void setFirstIndex(long index) {
        setLongValueByKey(FirstIndexKey, index);
    }

    private void setLongValueByKey(byte[] key, long index) {
        stateLock.writeLock().lock();
        try {
            database.put(key, ByteUtils.longToBytes(index));
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private long getLongValueByKey(byte[] key) {
        stateLock.readLock().lock();
        try {
            byte[] value = database.get(key);
            if (value == null) {
                throw new IllegalArgumentException("Key not found");
            }
            return ByteUtils.longFromBytes(value);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public long getCommitIndex() {
        return getLongValueByKey(CommitIndexKey);
    }

    @Override
    public long commit(long commitUpToIndex, StateMachine stateMachine) throws LogException {
        stateLock.writeLock().lock();
        try {
            if (commitUpToIndex > getCommitIndex()) {
                long newIndex = Math.min(commitUpToIndex, getLastIndex());

                for (long i = getCommitIndex() + 1; i <= newIndex; ++i) {
                    LogEntry logEntry = get(i);
                    if (logEntry.getType() == LogEntryType.StateMachineCommand) {
                        stateMachine.apply(logEntry.getEntry().toByteArray());
                    }
                }

                setCommitIndex(newIndex);
            }
            return getCommitIndex();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public long getLastIndex() {
        return getLongValueByKey(LastIndexKey);
    }

    @Override
    public long getFirstIndex() {
        return getLongValueByKey(FirstIndexKey);
    }

    @Override
    public LogEntry getLast() throws LogException {
        stateLock.readLock().lock();
        try {
            return get(getLastIndex());
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public boolean append(long prevLogIndex, long prevLogTerm, List<LogEntry> logEntries) throws LogException {
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
        WriteBatch wb = database.createWriteBatch();

        stateLock.writeLock().lock();
        try {
            if (nextEntry != null && nextEntry.getTerm() != newNextEntry.getTerm()) {
                long lastIndex = getLastIndex();
                for (long currIndex = nextEntryIndex; currIndex <= lastIndex; ++currIndex) {
                    wb.delete(ByteUtils.longToBytes(currIndex));
                }
            }

            long currIndex = nextEntryIndex;
            for (int j = 0; j < logEntries.size(); ++j) {
                wb.put(ByteUtils.longToBytes(currIndex + j), logEntries.get(j).toByteArray());
            }

            database.write(wb);
            setLastIndex(currIndex + logEntries.size() - 1);

            return true;
        } finally {
            stateLock.writeLock().unlock();
            try {
                wb.close();
            } catch (IOException e) {
                throw new LogException(e);
            }
        }
    }

    @Override
    public LogEntry get(long index) throws LogException {
        stateLock.readLock().lock();
        try {
            if (fakeIndex == index) {
                return LogEntryFactory.create(fakeTerm);
            }

            byte[] logEntry = database.get(ByteUtils.longToBytes(index));
            if (logEntry == null) {
                return null;
            }
            return LogEntry.parseFrom(logEntry);
        } catch (InvalidProtocolBufferException e) {
            throw new LogException(e);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public List<LogEntry> get(long nextIndex, int maxNum) throws LogException {
        long lastLogIndex = getLastIndex();

        ArrayList<LogEntry> retArr = new ArrayList<LogEntry>();
        if (nextIndex > lastLogIndex || nextIndex < 0 || maxNum <= 0) {
            return retArr;
        }

        long currIndex = nextIndex;
        while (currIndex < nextIndex + maxNum) {
            byte[] value = database.get(ByteUtils.longToBytes(currIndex));
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
    public long append(LogEntry logEntry) {
        stateLock.writeLock().lock();
        try {
            long nextIndex = getLastIndex() + 1;
            database.put(ByteUtils.longToBytes(nextIndex), logEntry.toByteArray());
            setLastIndex(nextIndex);
            return nextIndex;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void initialize(Configuration configuration) throws IOException {
        if (database != null) {
            throw new IllegalStateException("Already initialized");
        }
        this.configuration = configuration;

        Options options = new Options();
        options.createIfMissing(true);
        databaseFile = new File(configuration.getLogUri());
        database = factory.open(databaseFile, options);

        if (isDatabaseInitialized()) {
            logger.info("{}: FirstIndex={}, LastIndex={}, CommitIndex={}, votedFor={}, term={}",
                    this, getFirstIndex(), getLastIndex(), getCommitIndex(), getVotedFor(), getTerm());
        } else {
            initializeDatabase();
        }
    }

    @Override
    public void delete() throws IOException {
        if (databaseFile != null) {
            FileUtils.deleteDirectory(databaseFile);
            logger.info("{} has been deleted: {}.", this, configuration.getLogUri());
        }
    }

    @Override
    public String toString() {
        return "LevelDB log";
    }

    @Override
    public void setTerm(int term) {
        database.put(TermKey, ByteUtils.intToBytes(term));
    }

    @Override
    public int getTerm() {
        byte[] value = database.get(TermKey);
        if (value == null) {
            return 0;
        }
        return ByteUtils.intFromBytes(value);
    }

    @Override
    public void setVotedFor(MemberId memberId) {
        if (memberId == null) {
            database.delete(VoteForKey);
        } else {
            database.put(VoteForKey, memberId.toString().getBytes());
        }
    }

    @Override
    public MemberId getVotedFor() {
        byte[] value = database.get(VoteForKey);
        if (value != null) {
            return MemberId.fromString(new String(value));
        }
        return null;
    }

    @Override
    public void truncate(long toIndex) throws LogException {
        if (toIndex > getCommitIndex()) {
            throw new LogException("toIndex > getCommitIndex()");
        }

        logger.info("{} truncating log up to {}.", this, toIndex);

        WriteBatch wb = database.createWriteBatch();
        try {
            long nextIndex = getFirstIndex();
            while (nextIndex < toIndex) {
                wb.delete(ByteUtils.longToBytes(nextIndex));
                nextIndex++;
            }
            database.write(wb);
            setFirstIndex(toIndex);
            logger.info("{} has been truncated. FirstIndex={}.", this, getFirstIndex());
        } finally {
            try {
                wb.close();
            } catch (IOException e) {
                throw new LogException(e);
            }
        }
    }

    @Override
    public void setFakeLogEntryAndCommit(long index, int term) {
        stateLock.writeLock().lock();
        try {
            this.fakeIndex = index;
            this.fakeTerm = term;
            setCommitIndex(fakeIndex);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public SnapshotDescriptor getSnapshotAndTruncate(StateMachine stateMachine) throws LogException {
        stateLock.writeLock().lock();
        OutputStream snapshotOutputStream = null;
        SnapshotDescriptor snapshotDescriptor = null;
        WritableSnapshot writableSnapshot = null;
        long commitIndex = 0;
        try {
            commitIndex = getCommitIndex();
            LogEntry logEntry = get(commitIndex);
            int term = logEntry.getTerm();
            snapshotDescriptor = new SnapshotDescriptor(configuration.getSnapshotFolderPath(), commitIndex, term);
            writableSnapshot = stateMachine.getWritableSnapshot();
        } catch (Exception e) {
            logger.error("Error producing snapshot. ", e);
            throw new LogException("Error producing snapshot. ", e);
        } finally {
            stateLock.writeLock().unlock();
        }

        try {
            logger.info("{} snapshot writing started. SnapshotDescriptor={}.", stateMachine, snapshotDescriptor);
            snapshotOutputStream = snapshotDescriptor.getOutputStream();
            writableSnapshot.write(snapshotOutputStream);
            logger.info("{} snapshot writing finished. File size {} bytes.", stateMachine, snapshotDescriptor.getSize());

            logger.info("{} compaction started.", this);
            truncate(commitIndex);
            logger.info("{} compaction finished.", this);

            return snapshotDescriptor;
        } catch (Exception e) {
            logger.error("Error producing snapshot. SnapshotDescriptor={}.", snapshotDescriptor, e);
            throw new LogException("Error producing snapshot. ", e);
        } finally {
            if (snapshotOutputStream != null) {
                try {
                    snapshotOutputStream.close();
                } catch (IOException e) {
                    logger.error("Error closing outputStream.", e);
                }
            }
        }
    }

    @Override
    public void installSnapshot(StateMachine stateMachine, SnapshotDescriptor snapshotDescriptor) throws LogException, SnapshotInstallException {
        stateLock.writeLock().lock();
        try {
            stateMachine.installSnapshot(snapshotDescriptor);
            setFakeLogEntryAndCommit(snapshotDescriptor.getIndex(), snapshotDescriptor.getTerm());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

}

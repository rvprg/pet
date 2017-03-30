package com.rvprg.raft.protocol.impl;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.sm.StateMachine;

public class LevelDBLogImpl implements Log {

    private DB database;

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getCommitIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int commit(int commitUpToIndex, StateMachine stateMachine) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getLastIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getFirstIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public LogEntry getLast() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean append(int prevLogIndex, int prevLogTerm, List<LogEntry> logEntries) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public LogEntry get(int index) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<LogEntry> get(int nextIndex, int maxNum) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int append(LogEntry logEntry) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void init(String name) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);

        database = factory.open(new File(name), options);
    }

}

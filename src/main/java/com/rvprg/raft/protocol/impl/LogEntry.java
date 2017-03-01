package com.rvprg.raft.protocol.impl;

import java.nio.ByteBuffer;

public class LogEntry {

    private final int term;
    private final ByteBuffer command;

    public LogEntry(int term, ByteBuffer command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return term;
    }

    public ByteBuffer getCommand() {
        return command;
    }

}

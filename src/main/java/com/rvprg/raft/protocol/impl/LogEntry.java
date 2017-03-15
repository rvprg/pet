package com.rvprg.raft.protocol.impl;

public class LogEntry {

    private final int term;
    private final byte[] command;

    public LogEntry(int term, byte[] command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return term;
    }

    public byte[] getCommand() {
        return command;
    }

    public boolean isNoop() {
        return command == null || command.length == 0;
    }

}

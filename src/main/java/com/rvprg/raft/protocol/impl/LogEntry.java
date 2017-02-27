package com.rvprg.raft.protocol.impl;

import com.rvprg.raft.sm.Command;

public class LogEntry {

    private final int term;
    private final Command command;

    public LogEntry(int term, Command command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return term;
    }

    public Command getCommand() {
        return command;
    }

}

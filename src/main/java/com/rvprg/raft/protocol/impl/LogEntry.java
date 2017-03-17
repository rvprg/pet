package com.rvprg.raft.protocol.impl;

import java.util.Arrays;

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(command);
        result = prime * result + term;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LogEntry other = (LogEntry) obj;
        if (!Arrays.equals(command, other.command))
            return false;
        if (term != other.term)
            return false;
        return true;
    }

}

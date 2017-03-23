package com.rvprg.raft.protocol.impl;

import java.util.Arrays;

import com.rvprg.raft.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LogEntry {
    private final int term;
    @SuppressFBWarnings(justification = "Exposes internal buffer intentionally")
    private final byte[] command;
    private final LogEntryType type;

    public LogEntry(int term) {
        this(term, LogEntryType.NoOperationCommand, new byte[0]);
    }

    public LogEntry(int term, byte[] command) {
        this(term, LogEntryType.StateMachineCommand, command);
    }

    public LogEntry(int term, LogEntryType type, byte[] command) {
        this.term = term;
        this.command = command;
        this.type = type;
    }

    public int getTerm() {
        return term;
    }

    public byte[] getCommand() {
        return command;
    }

    public LogEntryType getType() {
        return type;
    }

    public boolean isNoOperationCommand() {
        return type == LogEntryType.NoOperationCommand;
    }

    public boolean isRaftProtocolCommand() {
        return type == LogEntryType.RaftProtocolCommand;
    }

    public boolean isStateMachineCommand() {
        return type == LogEntryType.StateMachineCommand;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(command);
        result = prime * result + term;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        if (type != other.type)
            return false;
        return true;
    }

}

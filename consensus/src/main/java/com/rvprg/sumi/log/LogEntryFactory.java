package com.rvprg.sumi.log;

import com.google.protobuf.ByteString;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry.LogEntryType;

public class LogEntryFactory {
    public static LogEntry create(int term, LogEntryType type, byte[] data) {
        return LogEntry.newBuilder().setTerm(term).setType(type).setEntry(ByteString.copyFrom(data)).build();
    }

    public static LogEntry create(int term, byte[] data) {
        return create(term, LogEntryType.StateMachineCommand, data);
    }

    public static LogEntry create(int term) {
        return create(term, LogEntryType.NoOperationCommand, new byte[0]);
    }

}

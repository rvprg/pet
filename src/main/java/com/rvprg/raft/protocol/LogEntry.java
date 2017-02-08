package com.rvprg.raft.protocol;

import com.rvprg.raft.sm.Command;

public interface LogEntry {
    int getTerm();

    Command getCommand();
}

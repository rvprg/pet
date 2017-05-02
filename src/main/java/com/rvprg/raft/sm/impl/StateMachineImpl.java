package com.rvprg.raft.sm.impl;

import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.sm.SnapshotWriter;

public class StateMachineImpl implements StateMachine {

    @Override
    public void apply(byte[] command) {
        // TODO
    }

    @Override
    public SnapshotWriter getSnapshotWriter() {
        // TODO
        throw new IllegalStateException("Not implemented");
    }

}

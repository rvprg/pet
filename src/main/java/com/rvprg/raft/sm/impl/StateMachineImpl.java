package com.rvprg.raft.sm.impl;

import com.rvprg.raft.sm.ReadableSnapshot;
import com.rvprg.raft.sm.SnapshotInstallException;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.sm.WritableSnapshot;

public class StateMachineImpl implements StateMachine {

    @Override
    public void apply(byte[] command) {
        // TODO
    }

    @Override
    public void installSnapshot(ReadableSnapshot snapshot) throws SnapshotInstallException {
        // TODO
    }

    @Override
    public WritableSnapshot getWritableSnapshot() {
        // TODO
        return null;
    }

}

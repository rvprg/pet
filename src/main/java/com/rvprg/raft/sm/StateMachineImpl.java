package com.rvprg.raft.sm;

import com.rvprg.raft.log.SnapshotInstallException;

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

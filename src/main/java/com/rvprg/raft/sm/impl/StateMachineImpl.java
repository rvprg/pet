package com.rvprg.raft.sm.impl;

import java.io.InputStream;

import com.rvprg.raft.sm.SnapshotInstallException;
import com.rvprg.raft.sm.StateMachine;

public class StateMachineImpl implements StateMachine {

    @Override
    public void apply(byte[] command) {
        // TODO
    }

    @Override
    public void installSnapshot(InputStream snapshot) throws SnapshotInstallException {
        // TODO Auto-generated method stub
    }
}

package com.rvprg.raft.tests.functional;

import com.google.inject.AbstractModule;
import com.rvprg.raft.log.LevelDBLogImpl;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.sm.StateMachine;

public class TestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(StateMachine.class).to(TestStateMachineImpl.class);
        bind(Log.class).to(LevelDBLogImpl.class);
    }
}

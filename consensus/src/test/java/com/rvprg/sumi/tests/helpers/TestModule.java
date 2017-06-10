package com.rvprg.sumi.tests.helpers;

import com.google.inject.AbstractModule;
import com.rvprg.sumi.log.LevelDBLogImpl;
import com.rvprg.sumi.log.Log;
import com.rvprg.sumi.sm.StateMachine;

public class TestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(StateMachine.class).to(TestStateMachineImpl.class);
        bind(Log.class).to(LevelDBLogImpl.class);
    }
}

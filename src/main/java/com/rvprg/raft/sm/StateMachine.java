package com.rvprg.raft.sm;

public interface StateMachine {
    void apply(Command command);
}

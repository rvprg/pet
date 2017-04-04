package com.rvprg.raft.protocol;

public interface CommandResult<T> extends LeaderHint {

    T getResult();

}

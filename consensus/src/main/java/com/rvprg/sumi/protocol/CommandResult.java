package com.rvprg.sumi.protocol;

public interface CommandResult<T> extends LeaderHint {

    T getResult();

}

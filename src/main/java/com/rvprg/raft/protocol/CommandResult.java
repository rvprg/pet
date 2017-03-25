package com.rvprg.raft.protocol;

import java.util.Optional;

public interface CommandResult<T> extends LeaderHint {

    Optional<T> getResult();

}

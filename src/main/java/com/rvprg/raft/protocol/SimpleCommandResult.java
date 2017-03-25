package com.rvprg.raft.protocol;

import java.util.Optional;

import com.rvprg.raft.transport.MemberId;

public class SimpleCommandResult<T> implements CommandResult<T> {
    private final Optional<T> result;
    private final Optional<MemberId> leaderId;

    @Override
    public Optional<MemberId> getLeaderId() {
        return leaderId;
    }

    @Override
    public Optional<T> getResult() {
        return result;
    }

    public SimpleCommandResult(T result, MemberId leaderId) {
        this.result = Optional.ofNullable(result);
        this.leaderId = Optional.ofNullable(leaderId);
    }
}

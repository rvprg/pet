package com.rvprg.raft.protocol.impl;

import java.util.Optional;

import com.rvprg.raft.transport.MemberId;

public class ApplyCommandResult {
    private final Optional<ApplyCommandFuture> result;
    private final Optional<MemberId> leaderId;

    public Optional<MemberId> getLeaderId() {
        return leaderId;
    }

    public Optional<ApplyCommandFuture> getResult() {
        return result;
    }

    public ApplyCommandResult(ApplyCommandFuture result, MemberId leaderId) {
        this.result = Optional.ofNullable(result);
        this.leaderId = Optional.ofNullable(leaderId);
    }

}

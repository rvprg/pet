package com.rvprg.raft.protocol.impl;

import com.rvprg.raft.transport.MemberId;

public class ApplyCommandResult {
    private final ApplyCommandFuture result;
    private final MemberId leaderId;

    public MemberId getLeaderId() {
        return leaderId;
    }

    public ApplyCommandFuture getResult() {
        return result;
    }

    public ApplyCommandResult(ApplyCommandFuture result, MemberId leaderId) {
        this.result = result;
        this.leaderId = leaderId;
    }

}

package com.rvprg.raft.protocol.impl;

import java.util.concurrent.CompletableFuture;

import com.rvprg.raft.transport.MemberId;

public class ApplyCommandResult {
    private final CompletableFuture<Integer> result;
    private final MemberId leaderId;

    public MemberId getLeaderId() {
        return leaderId;
    }

    public CompletableFuture<Integer> getResult() {
        return result;
    }

    public boolean isAccepted() {
        return result != null;
    }

    public ApplyCommandResult(CompletableFuture<Integer> f, MemberId leaderId) {
        this.result = f;
        this.leaderId = leaderId;
    }

}

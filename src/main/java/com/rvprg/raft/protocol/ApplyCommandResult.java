package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;

public class ApplyCommandResult extends SimpleCommandResult<ApplyCommandFuture> {

    public ApplyCommandResult(ApplyCommandFuture result, MemberId leaderId) {
        super(result, leaderId);
    }

}

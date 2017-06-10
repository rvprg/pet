package com.rvprg.sumi.protocol;

import com.rvprg.sumi.transport.MemberId;

public class ApplyCommandResult extends SimpleCommandResult<ApplyCommandFuture> {

    public ApplyCommandResult(ApplyCommandFuture result, MemberId leaderId) {
        super(result, leaderId);
    }

}

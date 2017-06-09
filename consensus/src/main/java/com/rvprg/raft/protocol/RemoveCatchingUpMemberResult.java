package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;

public class RemoveCatchingUpMemberResult extends SimpleCommandResult<Boolean> {

    public RemoveCatchingUpMemberResult(Boolean result, MemberId leaderId) {
        super(result, leaderId);
    }

}

package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;

public class AddCatchingUpMemberResult extends SimpleCommandResult<Boolean> {

    public AddCatchingUpMemberResult(Boolean result, MemberId leaderId) {
        super(result, leaderId);
    }

}

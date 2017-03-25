package com.rvprg.raft.protocol.impl;

import com.rvprg.raft.protocol.SimpleCommandResult;
import com.rvprg.raft.transport.MemberId;

public class RemoveCatchingUpMemberResult extends SimpleCommandResult<Boolean> {

    public RemoveCatchingUpMemberResult(Boolean result, MemberId leaderId) {
        super(result, leaderId);
    }

}

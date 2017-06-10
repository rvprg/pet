package com.rvprg.sumi.protocol;

import com.rvprg.sumi.transport.MemberId;

public class RemoveCatchingUpMemberResult extends SimpleCommandResult<Boolean> {

    public RemoveCatchingUpMemberResult(Boolean result, MemberId leaderId) {
        super(result, leaderId);
    }

}

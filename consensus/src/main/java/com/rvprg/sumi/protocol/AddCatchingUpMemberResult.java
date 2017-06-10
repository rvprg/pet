package com.rvprg.sumi.protocol;

import com.rvprg.sumi.transport.MemberId;

public class AddCatchingUpMemberResult extends SimpleCommandResult<Boolean> {

    public AddCatchingUpMemberResult(Boolean result, MemberId leaderId) {
        super(result, leaderId);
    }

}

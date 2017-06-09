package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;

public interface LeaderHint {

    MemberId getLeaderId();

}

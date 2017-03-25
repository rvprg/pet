package com.rvprg.raft.protocol;

import java.util.Optional;

import com.rvprg.raft.transport.MemberId;

public interface LeaderHint {

    Optional<MemberId> getLeaderId();

}

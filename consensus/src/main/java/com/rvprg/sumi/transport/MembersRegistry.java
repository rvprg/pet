package com.rvprg.sumi.transport;

import java.util.Set;

public interface MembersRegistry {
    ActiveMember get(final MemberId member);

    Set<ActiveMember> getAll();
}

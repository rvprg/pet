package com.rvprg.sumi.transport;

import java.util.Set;

public interface MembersRegistry {
    Member get(final MemberId member);

    Set<Member> getAll();
}

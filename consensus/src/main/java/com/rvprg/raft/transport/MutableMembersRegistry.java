package com.rvprg.raft.transport;

public interface MutableMembersRegistry extends MembersRegistry {
    void addMember(Member s);

    void removeMember(Member s);
}

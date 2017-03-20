package com.rvprg.raft.transport;

public interface EditableMembersRegistry extends MembersRegistry {
    void addMember(Member s);

    void removeMember(Member s);
}

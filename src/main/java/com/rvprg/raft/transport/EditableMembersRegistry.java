package com.rvprg.raft.transport;

public interface EditableMembersRegistry extends MembersRegistry {
    public void addMember(Member s);

    public void removeMember(Member s);
}

package com.rvprg.sumi.transport;

public interface MutableMembersRegistry extends MembersRegistry {
    void addMember(ActiveMember s);

    void removeMember(ActiveMember s);
}

package com.rvprg.raft.transport.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.transport.EditableMembersRegistry;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberId;

public class EditableMembersRegistryImpl implements EditableMembersRegistry {
    private final ConcurrentHashMap<MemberId, Member> members = new ConcurrentHashMap<MemberId, Member>();

    @Override
    public Member get(MemberId addr) {
        return members.get(addr);
    }

    @Override
    public void addMember(Member s) {
        members.put(s.getMemberId(), s);
    }

    @Override
    public void removeMember(Member s) {
        members.remove(s.getMemberId());
    }

    @Override
    public Set<Member> getAll() {
        return ImmutableSet.copyOf(members.values());
    }

}

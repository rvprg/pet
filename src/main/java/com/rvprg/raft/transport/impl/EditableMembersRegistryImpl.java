package com.rvprg.raft.transport.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.transport.EditableMembersRegistry;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberId;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class EditableMembersRegistryImpl implements EditableMembersRegistry {
    private final ConcurrentHashMap<MemberId, Member> members = new ConcurrentHashMap<MemberId, Member>();
    private ImmutableSet<Member> immutableSetOfMembers = ImmutableSet.of();

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    @Override
    public Member get(MemberId addr) {
        return members.get(addr);
    }

    @Override
    public void addMember(Member s) {
        stateLock.writeLock().lock();
        try {
            members.put(s.getMemberId(), s);
            immutableSetOfMembers = ImmutableSet.copyOf(members.values());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeMember(Member s) {
        stateLock.writeLock().lock();
        try {
            members.remove(s.getMemberId());
            immutableSetOfMembers = ImmutableSet.copyOf(members.values());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Set<Member> getAll() {
        stateLock.readLock().lock();
        try {
            return immutableSetOfMembers;
        } finally {
            stateLock.readLock().unlock();
        }
    }

}

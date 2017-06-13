package com.rvprg.sumi.transport;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableSet;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class MutableMembersRegistryImpl implements MutableMembersRegistry {
    private final ConcurrentHashMap<MemberId, ActiveMember> members = new ConcurrentHashMap<MemberId, ActiveMember>();
    private ImmutableSet<ActiveMember> immutableSetOfMembers = ImmutableSet.of();

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    @Override
    public ActiveMember get(MemberId addr) {
        return members.get(addr);
    }

    @Override
    public void addMember(ActiveMember s) {
        stateLock.writeLock().lock();
        try {
            members.put(s.getMemberId(), s);
            immutableSetOfMembers = ImmutableSet.copyOf(members.values());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeMember(ActiveMember s) {
        stateLock.writeLock().lock();
        try {
            members.remove(s.getMemberId());
            immutableSetOfMembers = ImmutableSet.copyOf(members.values());
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Set<ActiveMember> getAll() {
        stateLock.readLock().lock();
        try {
            return immutableSetOfMembers;
        } finally {
            stateLock.readLock().unlock();
        }
    }

}

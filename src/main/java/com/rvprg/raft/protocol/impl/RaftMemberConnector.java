package com.rvprg.raft.protocol.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberConnectorObserver;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MembersRegistry;

public class RaftMemberConnector implements MemberConnector {
    private final MemberConnector memberConnector;

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Set<MemberId> catchingUpMembers = new HashSet<>();

    public RaftMemberConnector(MemberConnector memberConnector) {
        this.memberConnector = memberConnector;
    }

    @Override
    public void register(final MemberId member, final MemberConnectorObserver observer) {
        memberConnector.register(member, observer);
    }

    @Override
    public void register(MemberId member) {
        memberConnector.register(member);
    }

    @Override
    public void unregister(MemberId member) {
        stateLock.readLock().lock();
        try {
            catchingUpMembers.remove(member);
            memberConnector.unregister(member);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void connect(MemberId member) {
        memberConnector.connect(member);
    }

    @Override
    public MembersRegistry getActiveMembers() {
        return memberConnector.getActiveMembers();
    }

    public Member getActiveMember(MemberId memberId) {
        return memberConnector.getActiveMembers().get(memberId);
    }

    public Set<Member> getAllActiveMembers() {
        return memberConnector.getActiveMembers().getAll();
    }

    public int getAllActiveMembersCount() {
        return getAllActiveMembers().size();
    }

    public Set<Member> getAllActiveVotingMembers() {
        stateLock.readLock().lock();
        try {
            return memberConnector
                    .getActiveMembers().getAll()
                    .stream()
                    .filter(x -> !catchingUpMembers.contains(x.getMemberId()))
                    .collect(Collectors.toSet());
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public Set<MemberId> getAllCatchingUpMemberIds() {
        stateLock.readLock().lock();
        try {
            return ImmutableSet.copyOf(catchingUpMembers);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void shutdown() {
        memberConnector.shutdown();
    }

    @Override
    public boolean isShutdown() {
        return memberConnector.isShutdown();
    }

    @Override
    public void connectAllRegistered() {
        memberConnector.connectAllRegistered();
    }

    @Override
    public Set<MemberId> getRegisteredMemberIds() {
        return memberConnector.getRegisteredMemberIds();
    }

    public int getVotingMembersCount() {
        stateLock.readLock().lock();
        try {
            return memberConnector.getRegisteredMemberIds().size() - catchingUpMembers.size();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public void registerAsCatchingUpMember(final MemberId memberId, final MemberConnectorObserver observer) {
        stateLock.writeLock().lock();
        try {
            catchingUpMembers.add(memberId);
            register(memberId, observer);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public void becomeVotingMember(final MemberId memberId) {
        stateLock.writeLock().lock();
        try {
            catchingUpMembers.remove(memberId);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public boolean isCatchingUpMember(final MemberId memberId) {
        stateLock.readLock().lock();
        try {
            return catchingUpMembers.contains(memberId);
        } finally {
            stateLock.readLock().unlock();
        }
    }

}

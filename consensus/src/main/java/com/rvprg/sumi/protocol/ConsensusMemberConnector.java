package com.rvprg.sumi.protocol;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.rvprg.sumi.transport.Member;
import com.rvprg.sumi.transport.MemberConnector;
import com.rvprg.sumi.transport.MemberConnectorListener;
import com.rvprg.sumi.transport.MemberConnectorListenerImpl;
import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.MembersRegistry;

public class ConsensusMemberConnector implements MemberConnector {
    private final MemberConnector memberConnector;

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Set<MemberId> catchingUpMembers = new HashSet<>();

    private final MemberConnectorListenerImpl memberConnectorListener;

    public ConsensusMemberConnector(MemberConnector memberConnector, MemberConnectorListenerImpl memberConnectorListener) {
        this.memberConnector = memberConnector;
        this.memberConnectorListener = memberConnectorListener;
    }

    public ConsensusMemberConnector(MemberConnector memberConnector) {
        this(memberConnector, null);
    }

    @Override
    public void register(final MemberId member, final MemberConnectorListener listener) {
        memberConnector.register(member, listener);
    }

    @Override
    public void register(MemberId member) {
        if (memberConnectorListener != null) {
            memberConnector.register(member, this.memberConnectorListener);
        } else {
            memberConnector.register(member);
        }
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

    public void registerAsCatchingUpMember(final MemberId memberId, final MemberConnectorListener listener) {
        stateLock.writeLock().lock();
        try {
            catchingUpMembers.add(memberId);
            register(memberId, listener);
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

    public void unregisterAllCatchingUpServers() {
        stateLock.writeLock().lock();
        try {
            for (MemberId memberId : new HashSet<MemberId>(catchingUpMembers)) {
                unregister(memberId);
            }
        } finally {
            stateLock.writeLock().unlock();
        }

    }

}

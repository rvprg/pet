package com.rvprg.raft.transport;

import java.util.Set;

public interface MemberConnector {
    public void register(final MemberId member, final MemberConnectorObserver observer);

    public void register(final MemberId member);

    public void unregister(final MemberId member);

    public void connect(final MemberId member);

    public Set<MemberId> getRegisteredMemberIds();

    public MembersRegistry getActiveMembers();

    public void shutdown();

    public boolean isShutdown();

    public void connectAllRegistered();
}

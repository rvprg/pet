package com.rvprg.raft.transport;

import java.util.Set;

public interface MemberConnector {
    void register(final MemberId member, final MemberConnectorObserver observer);

    void register(final MemberId member);

    void unregister(final MemberId member);

    void connect(final MemberId member);

    Set<MemberId> getRegisteredMemberIds();

    MembersRegistry getActiveMembers();

    void shutdown();

    boolean isShutdown();

    void connectAllRegistered();
}

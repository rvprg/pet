package com.rvprg.sumi.transport;

import java.util.Set;

public interface MemberConnector {
    void register(final MemberId member, final MemberConnectorListener listener);

    void register(final MemberId member);

    void unregister(final MemberId member);

    void connect(final MemberId member);

    Set<MemberId> getRegisteredMemberIds();

    MembersRegistry getActiveMembers();

    void shutdown();

    boolean isShutdown();

    void connectAllRegistered();
}

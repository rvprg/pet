package com.rvprg.sumi.transport;

public interface MemberConnectorListener {
    void connected(ActiveMember member);

    void scheduledReconnect(MemberId member);

    void disconnected(MemberId memberId);

    void exceptionCaught(MemberId memberId, Throwable cause);

    static MemberConnectorListener getDefaultInstance() {
        return new MemberConnectorListener() {

            @Override
            public void disconnected(MemberId memberId) {
                // nop
            }

            @Override
            public void connected(ActiveMember member) {
                // nop
            }

            @Override
            public void scheduledReconnect(MemberId member) {
                // nop
            }

            @Override
            public void exceptionCaught(MemberId memberId, Throwable cause) {
                // nop
            }
        };
    }
}

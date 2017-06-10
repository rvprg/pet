package com.rvprg.sumi.transport;

public interface MemberConnectorListener {
    void connected(Member member);

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
            public void connected(Member member) {
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

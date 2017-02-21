package com.rvprg.raft.transport;

public interface MemberConnectorObserver {
    public void connected(Member member);

    public void scheduledReconnect(MemberId member);

    public void disconnected(MemberId memberId);

    public void exceptionCaught(MemberId memberId, Throwable cause);

    static MemberConnectorObserver getDefaultInstance() {
        return new MemberConnectorObserver() {

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

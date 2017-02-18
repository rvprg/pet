package com.rvprg.raft.protocol;

public interface RaftObserver {
    void heartbeatTimedout();

    void nextElectionScheduled();

    void heartbeatReceived();

    void voteReceived();

    void voteRejected();

    void electionWon(int term);

    void electionTimedout();

    void started();

    void shutdown();

    static RaftObserver getDefaultInstance() {
        return new RaftObserver() {

            @Override
            public void heartbeatTimedout() {
                // nop
            }

            @Override
            public void nextElectionScheduled() {
                // nop
            }

            @Override
            public void heartbeatReceived() {
                // nop
            }

            @Override
            public void voteReceived() {
                // nop
            }

            @Override
            public void voteRejected() {
                // nop
            }

            @Override
            public void electionWon(int term) {
                // nop
            }

            @Override
            public void electionTimedout() {
                // nop
            }

            @Override
            public void started() {
                // nop
            }

            @Override
            public void shutdown() {
                // nop
            }

        };
    }
}

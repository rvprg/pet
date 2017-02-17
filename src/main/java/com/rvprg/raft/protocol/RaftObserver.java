package com.rvprg.raft.protocol;

public interface RaftObserver {
    void heartbeatTimedout();

    void nextElectionScheduled();

    void heartbeatReceived();

    void voteReceived();

    void voteRejected();

    void electionWon();

    void electionTimedout();

    void started();

    void shutdown();

    static RaftObserver getDefaultObserverInstance() {
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
            public void electionWon() {
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

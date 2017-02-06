package com.rvprg.raft.protocol;

public interface RaftObserver {
    void heartbeatTimedout();

    void nextElectionScheduled();

    void heartbeatReceived();

    void voteReceived();

    void electionWon();

    void electionTimedout();
}

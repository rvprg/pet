package com.rvprg.raft.protocol;

public interface RaftObserver {
    void electionInitiated();

    void nextElectionScheduled();

    void heartbeatReceived();
}

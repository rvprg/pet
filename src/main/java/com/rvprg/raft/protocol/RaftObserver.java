package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;

public interface RaftObserver {
    void heartbeatTimedout();

    void nextElectionScheduled();

    void heartbeatReceived();

    void voteReceived();

    void voteRejected();

    void electionWon(int term, Raft leader);

    void electionTimedout();

    void started();

    void shutdown();

    void appendEntriesRetryScheduled(MemberId memberId);

    static RaftObserver getDefaultInstance() {
        return new RaftObserverImpl();
    }
}

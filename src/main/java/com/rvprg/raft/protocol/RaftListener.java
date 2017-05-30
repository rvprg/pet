package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;

public interface RaftListener {
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

    static RaftListener getDefaultInstance() {
        return new RaftListenerImpl();
    }
}

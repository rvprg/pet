package com.rvprg.sumi.protocol;

import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.SnapshotDescriptor;

public interface ConsensusEventListener {
    void heartbeatTimedOut();

    void nextElectionScheduled();

    void heartbeatReceived();

    void voteReceived();

    void voteRejected();

    void electionWon(int term, Consensus leader);

    void electionTimedOut();

    void started();

    void shutdown();

    void appendEntriesRetryScheduled(MemberId memberId);

    void snapshotInstalled(SnapshotDescriptor descriptor);

    static ConsensusEventListener getDefaultInstance() {
        return new ConsensusEventListenerImpl();
    }
}

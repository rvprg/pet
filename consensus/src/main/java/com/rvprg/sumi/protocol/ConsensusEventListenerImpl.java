package com.rvprg.sumi.protocol;

import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.SnapshotDescriptor;

public class ConsensusEventListenerImpl implements ConsensusEventListener {

    @Override
    public void heartbeatTimedOut() {
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
    public void electionWon(int term, Consensus leader) {
        // nop
    }

    @Override
    public void electionTimedOut() {
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

    @Override
    public void appendEntriesRetryScheduled(MemberId memberId) {
        // nop
    }

    @Override
    public void snapshotInstalled(SnapshotDescriptor descriptor) {
        // nop
    }

}

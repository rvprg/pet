package com.rvprg.raft.protocol;

import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.SnapshotDescriptor;

public class RaftListenerImpl implements RaftListener {

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
    public void electionWon(int term, Raft leader) {
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

    @Override
    public void appendEntriesRetryScheduled(MemberId memberId) {
        // nop
    }

    @Override
    public void snapshotInstalled(SnapshotDescriptor descriptor) {
        // nop
    }

}

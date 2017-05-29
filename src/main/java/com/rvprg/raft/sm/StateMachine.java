package com.rvprg.raft.sm;

import com.rvprg.raft.log.SnapshotInstallException;

/**
 * All state machines implement this interface. The machines may implement
 * snapshot writing by implementing {@link Snapshotable}.
 */
public interface StateMachine {
    /**
     * Applies a state machine command. Interpretation of a command depends on
     * concrete implementation of the state machine.
     *
     * @param command
     *            State machine command.
     */
    void apply(byte[] command);

    /**
     * Initializes the state machine from the snapshot.
     *
     * @param snapshot
     */
    void installSnapshot(ReadableSnapshot snapshot) throws SnapshotInstallException;

    /**
     * Returns a writable snapshot.
     *
     * @return snapshot
     */
    WritableSnapshot getWritableSnapshot();
}

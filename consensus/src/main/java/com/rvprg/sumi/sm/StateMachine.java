package com.rvprg.sumi.sm;

import com.rvprg.sumi.log.SnapshotInstallException;

/**
 * All state machines implement this interface.
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
    void installSnapshot(StreambaleSnapshot snapshot) throws SnapshotInstallException;

    /**
     * Returns a writable snapshot.
     *
     * @return snapshot
     */
    WritableSnapshot getWritableSnapshot();
}

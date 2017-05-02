package com.rvprg.raft.sm;

/**
 * All state machines implement this interface. The machines may implement
 * snapshot writing by returning an implementation of {@link SnapshotWriter}.
 * If an instance of {@link SnapshotWriter} is returned and begin() method has
 * been called on it the state machine must block all apply() method calls until
 * the snapshot writer is released by calling its end() method. begin() method
 * implementation must be such that when called all currently being processed
 * apply() invocations should finish before begin() method returns.
 *
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
     * @return Returns an instance of {@link SnapshotWriter}.
     */
    SnapshotWriter getSnapshotWriter();
}

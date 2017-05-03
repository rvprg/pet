package com.rvprg.raft.sm;

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
}

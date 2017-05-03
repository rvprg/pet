package com.rvprg.raft.sm;

import java.io.OutputStream;

/**
 * Writes state machine contents to a stream. Assumes write lock semantics.
 *
 */
public interface Snapshotable {
    /**
     * Acquires write lock on the underlying state machine. A call to
     * end() must be made after writing the snapshot.
     */
    void begin();

    /**
     * Writes state machine contents to the output stream.
     *
     * @param outputStream
     *            state machine contents will be written to this stream.
     * @return size of the file.
     */
    long write(OutputStream outputStream);

    /**
     * Releases write lock on the underlying state machine.
     */
    void end();
}

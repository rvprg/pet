package com.rvprg.raft.sm;

import java.io.IOException;
import java.io.OutputStream;

public interface WritableSnapshot {
    void write(OutputStream stream) throws IOException;
}

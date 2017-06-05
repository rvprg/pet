package com.rvprg.raft.sm;

import java.io.FileNotFoundException;
import java.io.InputStream;

public interface StreambaleSnapshot {
    InputStream getInputStream() throws FileNotFoundException;
}

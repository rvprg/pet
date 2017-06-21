package com.rvprg.sumi.sm;

import java.io.FileNotFoundException;
import java.io.InputStream;

public interface StreamableSnapshot {
    InputStream getInputStream() throws FileNotFoundException;
}

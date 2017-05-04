package com.rvprg.raft.transport.impl;

import java.io.File;

public class SnapshotDescriptor {
    private final File fileName;
    private final String snapshotId;

    public File getFileName() {
        return fileName;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public SnapshotDescriptor(File fileName, String snapshotId) {
        this.fileName = fileName;
        this.snapshotId = snapshotId;
    }
}

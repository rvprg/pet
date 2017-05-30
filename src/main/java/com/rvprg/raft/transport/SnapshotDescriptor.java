package com.rvprg.raft.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rvprg.raft.sm.ReadableSnapshot;

public class SnapshotDescriptor implements ReadableSnapshot {
    private final static Logger logger = LoggerFactory.getLogger(SnapshotDescriptor.class);

    private final SnapshotMetadata metadata;
    private final File folder;

    public SnapshotMetadata getMetadata() {
        return metadata;
    }

    public File getSnapshotFile() {
        return new File(folder, metadata.getSnapshotId());
    }

    public OutputStream getOutputStream() throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(getSnapshotFile(), false));
    }

    public InputStream getInputStream() throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(getSnapshotFile()));
    }

    @Override
    public InputStream read() throws FileNotFoundException {
        return getInputStream();
    }

    public SnapshotDescriptor(File folder, SnapshotMetadata metadata) {
        this.folder = folder;
        this.metadata = metadata;
    }

    public static SnapshotDescriptor getLatestSnapshotDescriptor(File folder) {
        Optional<SnapshotMetadata> optFile = Arrays.stream(folder
                .listFiles((dir, file) -> file.endsWith(SnapshotMetadata.FILE_EXTENTION)))
                .map(f -> {
                    try {
                        return SnapshotMetadata.Builder.fromFile(f).build();
                    } catch (Exception e) {
                        logger.error("Failed on parsing metadata file {}", f, e);
                        return null;
                    }
                })
                .filter(f -> f != null)
                .max(SnapshotMetadata::compare);
        if (optFile.isPresent()) {
            return new SnapshotDescriptor(folder, optFile.get());
        }
        return null;
    }

    @Override
    public String toString() {
        return metadata.toString();
    }

}

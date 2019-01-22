package com.rvprg.sumi.transport;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rvprg.sumi.sm.StreamableSnapshot;

public class SnapshotDescriptor implements StreamableSnapshot {
    private final static Logger logger = LoggerFactory.getLogger(SnapshotDescriptor.class);

    private final SnapshotMetadata metadata;
    private final File folder;

    public SnapshotMetadata getMetadata() {
        return metadata;
    }

    public File getSnapshotFile() {
        return new File(folder, metadata.getSnapshotId());
    }

    @Override
    public InputStream getInputStream() throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(getSnapshotFile()));
    }

    public SnapshotDescriptor(File folder, SnapshotMetadata metadata) {
        this.folder = folder;
        this.metadata = metadata;
    }

    public static SnapshotDescriptor getLatestSnapshotDescriptor(File folder) {
        Optional<SnapshotMetadata> optFile = Arrays.stream(folder
                .listFiles((dir, file) -> file.endsWith(SnapshotMetadata.FILE_EXTENSION)))
                .map(f -> {
                    try {
                        return SnapshotMetadata.Builder.fromFile(f).build();
                    } catch (Exception e) {
                        logger.error("Failed on parsing metadata file {}", f, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .max(SnapshotMetadata::compare);
        return optFile.map(snapshotMetadata -> new SnapshotDescriptor(folder, snapshotMetadata)).orElse(null);
    }

    @Override
    public String toString() {
        return metadata.toString();
    }

}

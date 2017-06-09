package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.io.Files;
import com.rvprg.raft.transport.SnapshotDescriptor;
import com.rvprg.raft.transport.SnapshotMetadata;

public class SnapshotDescriptorTest {

    @Test
    public void testCompare() throws JsonParseException, JsonMappingException, IOException {
        long index = 10;
        int term = 1;

        SnapshotMetadata m1 = new SnapshotMetadata.Builder().index(index).term(term).build();
        SnapshotMetadata m2 = new SnapshotMetadata.Builder().index(index + 1).term(term).build();

        assertEquals(-1, SnapshotMetadata.compare(m1, m2));

        m1 = new SnapshotMetadata.Builder().index(index).term(term).build();
        m2 = new SnapshotMetadata.Builder().index(index).term(term).build();
        assertEquals(0, SnapshotMetadata.compare(m1, m2));

        m1 = new SnapshotMetadata.Builder().index(index).term(term + 1).build();
        m2 = new SnapshotMetadata.Builder().index(index).term(term).build();
        assertEquals(1, SnapshotMetadata.compare(m1, m2));
    }

    @Test
    public void testMostRecent() throws IOException {
        File tempDir = Files.createTempDir();
        long index = 10;
        int term = 1;

        SnapshotMetadata m1 = new SnapshotMetadata.Builder().index(index).term(term).build();
        SnapshotMetadata m2 = new SnapshotMetadata.Builder().index(index).term(term + 1).build();

        m1.toFile(new File(tempDir, UUID.randomUUID() + SnapshotMetadata.FILE_EXTENTION));
        m2.toFile(new File(tempDir, UUID.randomUUID() + SnapshotMetadata.FILE_EXTENTION));

        SnapshotDescriptor latest = SnapshotDescriptor.getLatestSnapshotDescriptor(tempDir);
        assertEquals(latest.getMetadata(), m2);
    }
}

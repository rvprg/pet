package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.junit.Test;

import com.google.common.io.Files;
import com.rvprg.raft.transport.SnapshotDescriptor;

public class SnapshotDescriptorTest {
    @Test
    public void testBasic() {
        Random random = new Random();
        File tempDir = Files.createTempDir();
        SnapshotDescriptor snapshotDescriptor1 = new SnapshotDescriptor(tempDir, random.nextLong(), random.nextInt());
        SnapshotDescriptor snapshotDescriptor2 = new SnapshotDescriptor(snapshotDescriptor1.getSnapshotId());
        assertEquals(snapshotDescriptor1, snapshotDescriptor2);
    }

    @Test
    public void testCompare() {
        File tempDir = Files.createTempDir();
        long index = 10;
        int term = 1;
        SnapshotDescriptor snapshotDescriptor1 = new SnapshotDescriptor(tempDir, index, term);
        SnapshotDescriptor snapshotDescriptor2 = new SnapshotDescriptor(tempDir, index + 1, term);
        assertEquals(-1, SnapshotDescriptor.compare(snapshotDescriptor1, snapshotDescriptor2));

        snapshotDescriptor1 = new SnapshotDescriptor(tempDir, index, term);
        snapshotDescriptor2 = new SnapshotDescriptor(tempDir, index, term);
        assertEquals(0, SnapshotDescriptor.compare(snapshotDescriptor1, snapshotDescriptor2));

        snapshotDescriptor1 = new SnapshotDescriptor(tempDir, index, term + 1);
        snapshotDescriptor2 = new SnapshotDescriptor(tempDir, index, term);
        assertEquals(1, SnapshotDescriptor.compare(snapshotDescriptor1, snapshotDescriptor2));
    }

    @Test
    public void testMostRecent() throws IOException {
        File tempDir = Files.createTempDir();
        long index = 10;
        int term = 1;
        SnapshotDescriptor snapshot1 = new SnapshotDescriptor(tempDir, index, term);
        OutputStream out1 = snapshot1.getOutputStream();
        out1.close();
        SnapshotDescriptor snapshot2 = new SnapshotDescriptor(tempDir, index, term + 1);
        OutputStream out2 = snapshot2.getOutputStream();
        out2.close();

        SnapshotDescriptor latest = SnapshotDescriptor.getLatestSnapshotDescriptor(tempDir);
        assertEquals(latest, snapshot2);
    }
}

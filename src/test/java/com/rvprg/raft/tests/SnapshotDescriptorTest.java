package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Random;

import org.junit.Test;

import com.google.common.io.Files;
import com.rvprg.raft.sm.SnapshotDescriptor;

public class SnapshotDescriptorTest {
    @Test
    public void testBasic() {
        Random random = new Random();
        File tempDir = Files.createTempDir();
        SnapshotDescriptor snapshotDescriptor1 = new SnapshotDescriptor(tempDir, random.nextLong(), random.nextInt());
        SnapshotDescriptor snapshotDescriptor2 = new SnapshotDescriptor(snapshotDescriptor1.getSnapshotId());
        assertEquals(snapshotDescriptor1, snapshotDescriptor2);
    }
}

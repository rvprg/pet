package com.rvprg.sumi.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.io.Files;
import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.SnapshotMetadata;
import com.rvprg.sumi.transport.SnapshotMetadata.Builder;

public class SnapshotMetadataTest {
    @Test
    public void testSerializationDeserialization() throws JsonGenerationException, JsonMappingException, IOException {
        SnapshotMetadata.Builder builder1 = new Builder();
        builder1.index(123);
        builder1.term(456);
        Set<MemberId> members = new HashSet<MemberId>();
        members.add(new MemberId("localhost", 123));
        builder1.members(members);

        File file = new File(Files.createTempDir(), "snapshot.metadata");
        builder1.build().toFile(file);
        file.deleteOnExit();

        Builder builder2 = SnapshotMetadata.Builder.fromFile(file);
        SnapshotMetadata m1 = builder1.build();
        SnapshotMetadata m2 = builder2.build();

        assertEquals(m1, m2);
    }
}

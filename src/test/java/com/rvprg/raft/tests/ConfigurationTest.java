package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.io.Files;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.transport.MemberId;

public class ConfigurationTest {
    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_NoMemberId() {
        Configuration.newBuilder().build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_WrongElectionTimeouts() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).snapshotFolderPath(new File("c:\\adfadsf")).electionMaxTimeout(10).electionMinTimeout(20).build();
    }

    @Test
    public void testConditions_ShouldSucceed_Defaults() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build();
    }

    @Test
    public void testConditions_File() throws JsonGenerationException, JsonMappingException, IOException {
        File tmpDir = Files.createTempDir();
        Configuration c1 = Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build();
        File file = new File(tmpDir, "test.json");
        c1.toFile(file);
        Configuration c2 = Configuration.Builder.fromFile(file).build();
        assertEquals(c1, c2);
    }

}

package com.rvprg.sumi.tests;

import com.google.common.io.Files;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.transport.MemberId;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class ConfigurationTest {
    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_NoMemberId() {
        Configuration.newBuilder().build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_WrongElectionTimeouts() {
        Configuration.newBuilder().selfId(new MemberId("localhost", 1234)).electionMaxTimeout(10).electionMinTimeout(20).build();
    }

    @Test
    public void testConditions_ShouldSucceed_Defaults() {
        Configuration.newBuilder().selfId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build();
    }

    @Test
    public void testConditions_File() throws IOException {
        File tmpDir = Files.createTempDir();
        Configuration c1 = Configuration.newBuilder().selfId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build();
        File file = new File(tmpDir, "test.json");
        c1.toFile(file);
        Configuration c2 = Configuration.Builder.fromFile(file).build();
        assertEquals(c1, c2);
    }

}

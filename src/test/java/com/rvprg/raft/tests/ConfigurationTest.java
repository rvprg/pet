package com.rvprg.raft.tests;

import java.net.URI;

import org.junit.Test;

import com.google.common.base.VerifyException;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.transport.MemberId;

public class ConfigurationTest {
    @Test(expected = VerifyException.class)
    public void testConditions_ShouldFail_NoMemberId() {
        Configuration.newBuilder().build();
    }

    @Test(expected = VerifyException.class)
    public void testConditions_ShouldFail_WrongElectionTimeouts() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).electionMaxTimeout(10).electionMinTimeout(20).build();
    }

    @Test
    public void testConditions_ShouldSucceed_Defaults() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).logUri(URI.create("file:///test")).build();
    }
}

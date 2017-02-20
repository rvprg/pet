package com.rvprg.raft.tests;

import org.junit.Test;

import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.transport.MemberId;

public class ConfigurationTest {
    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_NoMemberId() {
        Configuration.newBuilder().build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_WrongElectionTimeouts() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).electionMaxTimeout(10).electionMinTimeout(20).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConditions_ShouldFail_WrongHeartbeatTimeout() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).heartbeatTimeout(10).heartbeatPeriod(20).build();
    }

    @Test
    public void testConditions_ShouldSucceed_Defaults() {
        Configuration.newBuilder().memberId(new MemberId("localhost", 1234)).build();
    }
}

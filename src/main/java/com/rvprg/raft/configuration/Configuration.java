package com.rvprg.raft.configuration;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.transport.MemberId;

public class Configuration {
    // TODO: Make proper builder-type configuration class.
    private String host;
    private int port;
    private int heartbeatTimeout = 200;
    private int heartbeatPeriod = 50;
    private int electionMinTimeout = 150;
    private int electionMaxTimeout = 300;

    public int getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public void setHeartbeatPeriod(int heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
    }

    public int getElectionMinTimeout() {
        return electionMinTimeout;
    }

    public void setElectionMinTimeout(int electionMinTimeout) {
        this.electionMinTimeout = electionMinTimeout;
    }

    public int getElectionMaxTimeout() {
        return electionMaxTimeout;
    }

    public void setElectionMaxTimeout(int electionMaxTimeout) {
        this.electionMaxTimeout = electionMaxTimeout;
    }

    private final Set<MemberId> nodes = new HashSet<MemberId>();

    public Set<MemberId> getMembers() {
        return ImmutableSet.copyOf(nodes);
    }

    public void addMember(MemberId member) {
        nodes.add(member);
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(int heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}

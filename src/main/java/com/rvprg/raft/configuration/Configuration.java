package com.rvprg.raft.configuration;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.transport.MemberId;

public class Configuration {
    private String host;
    private int port;
    private int heartbeatTimeout;
    private final Set<MemberId> nodes = new HashSet<MemberId>();

    public Set<MemberId> getMember() {
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

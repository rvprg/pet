package com.rvprg.raft.configuration;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.transport.MemberId;

import net.jcip.annotations.Immutable;

@Immutable
public class Configuration {

    public static class Builder {
        private MemberId memberId;
        private int heartbeatTimeout = 200;
        private int heartbeatPeriod = 50;
        private int electionMinTimeout = 150;
        private int electionMaxTimeout = 300;
        private Set<MemberId> memberIds = new HashSet<MemberId>();

        public Builder memberId(MemberId memberId) {
            this.memberId = memberId;
            return this;
        }

        public Builder heartbeatTimeout(int heartbeatTimeout) {
            this.heartbeatTimeout = heartbeatTimeout;
            return this;
        }

        public Builder heartbeatPeriod(int heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public Builder electionMinTimeout(int electionMinTimeout) {
            this.electionMinTimeout = electionMinTimeout;
            return this;
        }

        public Builder electionMaxTimeout(int electionMaxTimeout) {
            this.electionMaxTimeout = electionMaxTimeout;
            return this;
        }

        public Builder addMemberIds(Set<MemberId> memberIds) {
            this.memberIds.addAll(memberIds);
            return this;
        }

        public Configuration build() {
            return new Configuration(this);
        }

    }

    private final MemberId memberId;
    private final int heartbeatTimeout;
    private final int heartbeatPeriod;
    private final int electionMinTimeout;
    private final int electionMaxTimeout;
    private final Set<MemberId> memberIds;

    public Configuration(Builder builder) {
        memberId = builder.memberId;
        heartbeatTimeout = builder.heartbeatTimeout;
        heartbeatPeriod = builder.heartbeatPeriod;
        electionMinTimeout = builder.electionMinTimeout;
        electionMaxTimeout = builder.electionMaxTimeout;
        memberIds = ImmutableSet.copyOf(builder.memberIds);
    }

    public MemberId getMemberId() {
        return memberId;
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public int getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public int getElectionMinTimeout() {
        return electionMinTimeout;
    }

    public int getElectionMaxTimeout() {
        return electionMaxTimeout;
    }

    public Set<MemberId> getMemberIds() {
        return memberIds;
    }

    public static Builder newBuilder() {
        return new Builder();
    }
}

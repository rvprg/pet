package com.rvprg.raft.configuration;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Verify;
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
        private int autoReconnectRetryInterval = 1000;
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

        public Builder autoReconnectInterval(int autoReconnectInterval) {
            this.autoReconnectRetryInterval = autoReconnectInterval;
            return this;
        }

        public Builder addMemberIds(Set<MemberId> memberIds) {
            this.memberIds.addAll(memberIds);
            return this;
        }

        public Configuration build() {
            Verify.verify(autoReconnectRetryInterval > 0, "autoReconnectRetryInterval must be positive");
            Verify.verify(electionMinTimeout > 0, "electionMinTimeout must be positive");
            Verify.verify(electionMaxTimeout > 0, "electionMaxTimeout must be positive");
            Verify.verify(heartbeatPeriod > 0, "heartbeatPeriod must be positive");
            Verify.verify(electionMinTimeout < electionMaxTimeout, "electionMaxTimeout must not be smaller than election electionMinTimeout");
            Verify.verify(heartbeatTimeout > heartbeatPeriod, "heartbeatTimeout must not be smaller than heartbeatPeriod");
            Verify.verify(memberId != null, "memberId must not be null");
            return new Configuration(this);
        }

    }

    private final MemberId memberId;
    private final int heartbeatTimeout;
    private final int heartbeatPeriod;
    private final int electionMinTimeout;
    private final int electionMaxTimeout;
    private final int autoReconnectRetryInterval;
    private final Set<MemberId> memberIds;

    public Configuration(Builder builder) {
        memberId = builder.memberId;
        heartbeatTimeout = builder.heartbeatTimeout;
        heartbeatPeriod = builder.heartbeatPeriod;
        electionMinTimeout = builder.electionMinTimeout;
        electionMaxTimeout = builder.electionMaxTimeout;
        autoReconnectRetryInterval = builder.autoReconnectRetryInterval;
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

    public int getAutoReconnectRetryInterval() {
        return autoReconnectRetryInterval;
    }

}

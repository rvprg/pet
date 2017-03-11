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
        private int heartbeatInterval = 50;
        private int electionMinTimeout = 150;
        private int electionMaxTimeout = 300;
        private int autoReconnectRetryInterval = 1000;
        private int replicationRetryInterval = 500;
        private int maxNumberOfLogEntriesPerRequest = 3;
        private Set<MemberId> memberIds = new HashSet<MemberId>();

        public Builder memberId(MemberId memberId) {
            this.memberId = memberId;
            return this;
        }

        public Builder heartbeatInterval(int heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
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

        public Builder setReplicationRetryInterval(int replicationRetryInterval) {
            this.replicationRetryInterval = replicationRetryInterval;
            return this;
        }

        public Builder setMaxNumberOfLogEntriesPerRequest(int maxNumberOfLogEntriesPerRequest) {
            this.maxNumberOfLogEntriesPerRequest = maxNumberOfLogEntriesPerRequest;
            return this;
        }

        public Configuration build() {
            Verify.verify(autoReconnectRetryInterval > 0, "autoReconnectRetryInterval must be positive and nonzero");
            Verify.verify(replicationRetryInterval > 0, "replicationRetryInterval must be positive and nonzero");
            Verify.verify(electionMinTimeout > 0, "electionMinTimeout must be positive and nonzero");
            Verify.verify(electionMaxTimeout > 0, "electionMaxTimeout must be positive and nonzero");
            Verify.verify(maxNumberOfLogEntriesPerRequest > 0, "maxNumberOfLogEntriesPerRequest must be positive and nonzero");
            Verify.verify(heartbeatInterval > 0, "heartbeatInterval must be positive and nonzero");
            Verify.verify(electionMinTimeout < electionMaxTimeout, "electionMaxTimeout must not be smaller than election electionMinTimeout");
            Verify.verify(memberId != null, "memberId must not be null");
            return new Configuration(this);
        }

    }

    private final MemberId memberId;
    private final int heartbeatInterval;
    private final int electionMinTimeout;
    private final int electionMaxTimeout;
    private final int autoReconnectRetryInterval;
    private final Set<MemberId> memberIds;
    private final int replicationRetryInterval;
    private final int maxNumberOfLogEntriesPerRequest;

    public int getMaxNumberOfLogEntriesPerRequest() {
        return maxNumberOfLogEntriesPerRequest;
    }

    public Configuration(Builder builder) {
        memberId = builder.memberId;
        heartbeatInterval = builder.heartbeatInterval;
        electionMinTimeout = builder.electionMinTimeout;
        electionMaxTimeout = builder.electionMaxTimeout;
        autoReconnectRetryInterval = builder.autoReconnectRetryInterval;
        replicationRetryInterval = builder.replicationRetryInterval;
        maxNumberOfLogEntriesPerRequest = builder.maxNumberOfLogEntriesPerRequest;
        memberIds = ImmutableSet.copyOf(builder.memberIds);
    }

    public MemberId getMemberId() {
        return memberId;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
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

    public int getReplicationRetryInterval() {
        return replicationRetryInterval;
    }

}

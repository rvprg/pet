package com.rvprg.raft.configuration;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
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
        private int logCompactionThreshold = 100;
        private int mainEventLoopThreadPoolSize = 0;
        private int memberConnectorEventLoopThreadPoolSize = 0;
        private int messageReceiverBossEventLoopThreadPoolSize = 0;
        private int messageReceiverWorkerEventLoopThreadPoolSize = 0;
        private File snapshotFolderPath = Files.createTempDir();

        private URI logUri;
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

        public Builder replicationRetryInterval(int replicationRetryInterval) {
            this.replicationRetryInterval = replicationRetryInterval;
            return this;
        }

        public Builder maxNumberOfLogEntriesPerRequest(int maxNumberOfLogEntriesPerRequest) {
            this.maxNumberOfLogEntriesPerRequest = maxNumberOfLogEntriesPerRequest;
            return this;
        }

        public Builder logCompactionThreshold(int logCompactionThreshold) {
            this.logCompactionThreshold = logCompactionThreshold;
            return this;
        }

        public Builder logUri(URI logUri) {
            this.logUri = logUri;
            return this;
        }

        public Builder mainEventLoopThreadPoolSize(int mainEventLoopThreadPoolSize) {
            this.mainEventLoopThreadPoolSize = mainEventLoopThreadPoolSize;
            return this;
        }

        public Builder memberConnectorEventLoopThreadPoolSize(int memberConnectorEventLoopThreadPoolSize) {
            this.memberConnectorEventLoopThreadPoolSize = memberConnectorEventLoopThreadPoolSize;
            return this;
        }

        public Builder messageReceiverBossEventLoopThreadPoolSize(int messageReceiverBossEventLoopThreadPoolSize) {
            this.messageReceiverBossEventLoopThreadPoolSize = messageReceiverBossEventLoopThreadPoolSize;
            return this;
        }

        public Builder messageReceiverWorkerEventLoopThreadPoolSize(int messageReceiverWorkerEventLoopThreadPoolSize) {
            this.messageReceiverWorkerEventLoopThreadPoolSize = messageReceiverWorkerEventLoopThreadPoolSize;
            return this;
        }

        public Builder snapshotFolderPath(File snapshotFolderPath) {
            this.snapshotFolderPath = snapshotFolderPath;
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
            Verify.verify(logCompactionThreshold >= 0, "logCompactionThreshold must be positive or zero");
            Verify.verify(mainEventLoopThreadPoolSize >= 0, "mainEventLoopThreadPoolSize must be positive");
            Verify.verify(memberConnectorEventLoopThreadPoolSize >= 0, "memberConnectorEventLoopThreadPoolSize must be positive");
            Verify.verify(messageReceiverBossEventLoopThreadPoolSize >= 0, "messageReceiverBossEventLoopThreadPoolSize must be positive");
            Verify.verify(messageReceiverWorkerEventLoopThreadPoolSize >= 0, "messageReceiverWorkerEventLoopThreadPoolSize must be positive");
            Verify.verify(memberId != null, "memberId must not be null");
            Verify.verify(logUri != null, "logUri must not be null");
            Verify.verify(snapshotFolderPath != null, "snapshotFolderPath must not be null");
            Verify.verify(snapshotFolderPath.exists() && snapshotFolderPath.isDirectory() && snapshotFolderPath.canWrite(),
                    "snapshotFolderPath should point to existing folder and be writable");
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
    private final int logCompactionThreshold;
    private final URI logUri;
    private final int mainEventLoopThreadPoolSize;
    private final int memberConnectorEventLoopThreadPoolSize;
    private final int messageReceiverBossEventLoopThreadPoolSize;
    private final int messageReceiverWorkerEventLoopThreadPoolSize;
    private final File snapshotFolderPath;

    public Configuration(Builder builder) {
        memberId = builder.memberId;
        heartbeatInterval = builder.heartbeatInterval;
        electionMinTimeout = builder.electionMinTimeout;
        electionMaxTimeout = builder.electionMaxTimeout;
        autoReconnectRetryInterval = builder.autoReconnectRetryInterval;
        replicationRetryInterval = builder.replicationRetryInterval;
        maxNumberOfLogEntriesPerRequest = builder.maxNumberOfLogEntriesPerRequest;
        memberIds = ImmutableSet.copyOf(builder.memberIds);
        logCompactionThreshold = builder.logCompactionThreshold;
        logUri = builder.logUri;
        mainEventLoopThreadPoolSize = builder.mainEventLoopThreadPoolSize;
        memberConnectorEventLoopThreadPoolSize = builder.memberConnectorEventLoopThreadPoolSize;
        messageReceiverBossEventLoopThreadPoolSize = builder.messageReceiverBossEventLoopThreadPoolSize;
        messageReceiverWorkerEventLoopThreadPoolSize = builder.messageReceiverWorkerEventLoopThreadPoolSize;
        snapshotFolderPath = builder.snapshotFolderPath;
    }

    public int getMessageReceiverWorkerEventLoopThreadPoolSize() {
        return messageReceiverWorkerEventLoopThreadPoolSize;
    }

    public int getMaxNumberOfLogEntriesPerRequest() {
        return maxNumberOfLogEntriesPerRequest;
    }

    public int getMemberConnectorEventLoopThreadPoolSize() {
        return memberConnectorEventLoopThreadPoolSize;
    }

    public int getMessageReceiverBossEventLoopThreadPoolSize() {
        return messageReceiverBossEventLoopThreadPoolSize;
    }

    public int getMainEventLoopThreadPoolSize() {
        return mainEventLoopThreadPoolSize;
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

    public int getLogCompactionThreshold() {
        return logCompactionThreshold;
    }

    public URI getLogUri() {
        return logUri;
    }

    public File getSnapshotFolderPath() {
        return snapshotFolderPath;
    }

}

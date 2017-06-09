package com.rvprg.raft.configuration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.rvprg.raft.transport.MemberId;

import net.jcip.annotations.Immutable;

@Immutable
@JsonInclude(Include.NON_NULL)
@ValidConfiguration
public class Configuration {
    @JsonInclude(Include.NON_NULL)
    public static class Builder {
        @JsonProperty("selfId")
        private MemberId selfId;
        @JsonProperty("heartbeatInterval")
        private int heartbeatInterval = 50;
        @JsonProperty("electionMinTimeout")
        private int electionMinTimeout = 150;
        @JsonProperty("electionMaxTimeout")
        private int electionMaxTimeout = 300;
        @JsonProperty("autoReconnectRetryInterval")
        private int autoReconnectRetryInterval = 1000;
        @JsonProperty("replicationRetryInterval")
        private int replicationRetryInterval = 500;
        @JsonProperty("maxNumberOfLogEntriesPerRequest")
        private int maxNumberOfLogEntriesPerRequest = 3;
        @JsonProperty("logCompactionThreshold")
        private int logCompactionThreshold = 100;
        @JsonProperty("mainEventLoopThreadPoolSize")
        private int mainEventLoopThreadPoolSize = 0;
        @JsonProperty("memberConnectorEventLoopThreadPoolSize")
        private int memberConnectorEventLoopThreadPoolSize = 0;
        @JsonProperty("messageReceiverBossEventLoopThreadPoolSize")
        private int messageReceiverBossEventLoopThreadPoolSize = 0;
        @JsonProperty("messageReceiverWorkerEventLoopThreadPoolSize")
        private int messageReceiverWorkerEventLoopThreadPoolSize = 0;
        @JsonProperty("snapshotFolderPath")
        private File snapshotFolderPath = Files.createTempDir();
        @JsonProperty("snapshotSenderPort")
        private int snapshotSenderPort = 10000;
        @JsonProperty("logUri")
        private URI logUri;
        @JsonProperty("memberIds")
        private Set<MemberId> memberIds = new HashSet<MemberId>();

        public Builder selfId(MemberId selfId) {
            this.selfId = selfId;
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

        public Builder snapshotSenderPort(int snapshotSenderPort) {
            this.snapshotSenderPort = snapshotSenderPort;
            return this;
        }

        public static Builder fromFile(File file) throws JsonParseException, JsonMappingException, IOException {
            return getMapper().readValue(file, Builder.class);
        }

        public Configuration build() {
            ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
            Validator validator = factory.getValidator();

            Configuration configuration = new Configuration(this);
            Set<ConstraintViolation<Configuration>> res = validator.validate(configuration);
            if (res.size() > 0) {
                String errorMessage = Joiner.on(";").join(
                        res.stream().map(x -> x.getPropertyPath() + " " + x.getMessage()).collect(Collectors.toList()));
                throw new IllegalArgumentException(errorMessage);
            }

            return configuration;
        }

    }

    @NotNull
    @JsonProperty("selfId")
    private final MemberId selfId;

    @JsonProperty("heartbeatInterval")
    private final int heartbeatInterval;

    @DecimalMin(value = "1")
    @JsonProperty("electionMinTimeout")
    private final int electionMinTimeout;
    @DecimalMin(value = "1")
    @JsonProperty("electionMaxTimeout")
    private final int electionMaxTimeout;

    @DecimalMin(value = "1")
    @JsonProperty("autoReconnectRetryInterval")
    private final int autoReconnectRetryInterval;

    @NotNull
    @JsonProperty("memberIds")
    private final Set<MemberId> memberIds;

    @DecimalMin(value = "1")
    @JsonProperty("replicationRetryInterval")
    private final int replicationRetryInterval;

    @DecimalMin(value = "1")
    @JsonProperty("maxNumberOfLogEntriesPerRequest")
    private final int maxNumberOfLogEntriesPerRequest;

    @DecimalMin(value = "0")
    @JsonProperty("logCompactionThreshold")
    private final int logCompactionThreshold;

    @NotNull
    @JsonProperty("logUri")
    private final URI logUri;

    @DecimalMin(value = "0")
    @JsonProperty("mainEventLoopThreadPoolSize")
    private final int mainEventLoopThreadPoolSize;

    @DecimalMin(value = "0")
    @JsonProperty("memberConnectorEventLoopThreadPoolSize")
    private final int memberConnectorEventLoopThreadPoolSize;

    @DecimalMin(value = "0")
    @JsonProperty("messageReceiverBossEventLoopThreadPoolSize")
    private final int messageReceiverBossEventLoopThreadPoolSize;

    @DecimalMin(value = "0")
    @JsonProperty("messageReceiverWorkerEventLoopThreadPoolSize")
    private final int messageReceiverWorkerEventLoopThreadPoolSize;

    @NotNull
    @JsonProperty("snapshotFolderPath")
    private final File snapshotFolderPath;

    @DecimalMin(value = "1")
    @JsonProperty("snapshotSenderPort")
    private final int snapshotSenderPort;

    public Configuration(Builder builder) {
        selfId = builder.selfId;
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
        snapshotSenderPort = builder.snapshotSenderPort;
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

    public MemberId getSelfId() {
        return selfId;
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

    public int getSnapshotSenderPort() {
        return snapshotSenderPort;
    }

    public void toFile(File file) throws JsonGenerationException, JsonMappingException, IOException {
        getMapper().writeValue(file, this);
    }

    private static ObjectMapper getMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + autoReconnectRetryInterval;
        result = prime * result + electionMaxTimeout;
        result = prime * result + electionMinTimeout;
        result = prime * result + heartbeatInterval;
        result = prime * result + logCompactionThreshold;
        result = prime * result + ((logUri == null) ? 0 : logUri.hashCode());
        result = prime * result + mainEventLoopThreadPoolSize;
        result = prime * result + maxNumberOfLogEntriesPerRequest;
        result = prime * result + memberConnectorEventLoopThreadPoolSize;
        result = prime * result + ((selfId == null) ? 0 : selfId.hashCode());
        result = prime * result + ((memberIds == null) ? 0 : memberIds.hashCode());
        result = prime * result + messageReceiverBossEventLoopThreadPoolSize;
        result = prime * result + messageReceiverWorkerEventLoopThreadPoolSize;
        result = prime * result + replicationRetryInterval;
        result = prime * result + ((snapshotFolderPath == null) ? 0 : snapshotFolderPath.hashCode());
        result = prime * result + snapshotSenderPort;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Configuration other = (Configuration) obj;
        if (autoReconnectRetryInterval != other.autoReconnectRetryInterval)
            return false;
        if (electionMaxTimeout != other.electionMaxTimeout)
            return false;
        if (electionMinTimeout != other.electionMinTimeout)
            return false;
        if (heartbeatInterval != other.heartbeatInterval)
            return false;
        if (logCompactionThreshold != other.logCompactionThreshold)
            return false;
        if (logUri == null) {
            if (other.logUri != null)
                return false;
        } else if (!logUri.equals(other.logUri))
            return false;
        if (mainEventLoopThreadPoolSize != other.mainEventLoopThreadPoolSize)
            return false;
        if (maxNumberOfLogEntriesPerRequest != other.maxNumberOfLogEntriesPerRequest)
            return false;
        if (memberConnectorEventLoopThreadPoolSize != other.memberConnectorEventLoopThreadPoolSize)
            return false;
        if (selfId == null) {
            if (other.selfId != null)
                return false;
        } else if (!selfId.equals(other.selfId))
            return false;
        if (memberIds == null) {
            if (other.memberIds != null)
                return false;
        } else if (!memberIds.equals(other.memberIds))
            return false;
        if (messageReceiverBossEventLoopThreadPoolSize != other.messageReceiverBossEventLoopThreadPoolSize)
            return false;
        if (messageReceiverWorkerEventLoopThreadPoolSize != other.messageReceiverWorkerEventLoopThreadPoolSize)
            return false;
        if (replicationRetryInterval != other.replicationRetryInterval)
            return false;
        if (snapshotFolderPath == null) {
            if (other.snapshotFolderPath != null)
                return false;
        } else if (!snapshotFolderPath.equals(other.snapshotFolderPath))
            return false;
        if (snapshotSenderPort != other.snapshotSenderPort)
            return false;
        return true;
    }

}

package com.rvprg.sumi.transport;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableSet;
import com.rvprg.raft.protocol.messages.ProtocolMessages.SnapshotDownloadRequest;

@JsonInclude(Include.NON_NULL)
public class SnapshotMetadata {
    public static final String FILE_EXTENTION = ".json";

    @JsonInclude(Include.NON_NULL)
    public static class Builder {
        @JsonProperty("index")
        private long index;
        @JsonProperty("term")
        private int term;
        @JsonProperty("snapshotId")
        private String snapshotId;
        @JsonProperty("members")
        private Set<MemberId> members = new HashSet<MemberId>();
        @JsonProperty("size")
        private long size;

        public static Builder from(SnapshotMetadata metadata) {
            Builder b = new Builder();
            b.index = metadata.index;
            b.size = metadata.size;
            b.snapshotId = metadata.snapshotId;
            b.members = ImmutableSet.copyOf(metadata.members);
            return b;
        }

        public Builder index(long index) {
            this.index = index;
            return this;
        }

        public Builder term(int term) {
            this.term = term;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder snapshotId(String uniqueId) {
            this.snapshotId = uniqueId;
            return this;
        }

        public Builder members(Set<MemberId> members) {
            this.members = members;
            return this;
        }

        public SnapshotMetadata build() {
            return new SnapshotMetadata(this);
        }

        public static Builder fromFile(File file) throws JsonParseException, JsonMappingException, IOException {
            return getMapper().readValue(file, Builder.class);
        }

    }

    public SnapshotMetadata(Builder builder) {
        this.index = builder.index;
        this.term = builder.term;
        this.snapshotId = builder.snapshotId;
        this.size = builder.size;
        this.members = ImmutableSet.<MemberId> copyOf(builder.members);
    }

    public void toFile(File file) throws JsonGenerationException, JsonMappingException, IOException {
        getMapper().writeValue(file, this);
    }

    @JsonProperty("index")
    private final long index;
    @JsonProperty("term")
    private final int term;
    @JsonProperty("snapshotId")
    private final String snapshotId;
    @JsonProperty("members")
    private final Set<MemberId> members;
    @JsonProperty("size")
    private final long size;

    public long getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public Set<MemberId> getMembers() {
        return members;
    }

    public long getSize() {
        return size;
    }

    public static SnapshotMetadata fromRequest(SnapshotDownloadRequest request) {
        Builder builder = new Builder();
        builder.index = request.getIndex();
        builder.term = request.getTerm();
        builder.snapshotId = request.getSnapshotId();
        builder.size = request.getSize();
        Set<MemberId> m = new HashSet<MemberId>();
        for (int i = 0; i < request.getMembersCount(); ++i) {
            m.add(MemberId.fromString(request.getMembers(i)));
        }
        builder.members = ImmutableSet.<MemberId> copyOf(m);
        return builder.build();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (index ^ (index >>> 32));
        result = prime * result + ((members == null) ? 0 : members.hashCode());
        result = prime * result + (int) (size ^ (size >>> 32));
        result = prime * result + ((snapshotId == null) ? 0 : snapshotId.hashCode());
        result = prime * result + term;
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
        SnapshotMetadata other = (SnapshotMetadata) obj;
        if (index != other.index)
            return false;
        if (members == null) {
            if (other.members != null)
                return false;
        } else if (!members.equals(other.members))
            return false;
        if (size != other.size)
            return false;
        if (snapshotId == null) {
            if (other.snapshotId != null)
                return false;
        } else if (!snapshotId.equals(other.snapshotId))
            return false;
        if (term != other.term)
            return false;
        return true;
    }

    public static int compare(SnapshotMetadata s1, SnapshotMetadata s2) {
        if (s1.term == s2.term) {
            return Long.compare(s1.index, s2.index);
        }
        return Integer.compare(s1.term, s2.term);
    }

    public static int compare(File f1, File f2) throws JsonParseException, JsonMappingException, IOException {
        SnapshotMetadata m1 = Builder.fromFile(f1).build();
        SnapshotMetadata m2 = Builder.fromFile(f2).build();
        return compare(m1, m2);
    }

    private static ObjectMapper getMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        return mapper;
    }

    @Override
    public String toString() {
        return "SnapshotMetadata [index=" + index + ", term=" + term + ", snapshotId=" + snapshotId + ", size=" + size + "]";
    }

}

package com.rvprg.raft.protocol.impl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.rvprg.raft.transport.CustomMemberIdDeserializer;
import com.rvprg.raft.transport.CustomMemberIdSerializer;
import com.rvprg.raft.transport.MemberId;

@JsonInclude(Include.NON_NULL)
public class RaftProtocolCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Command {
        AddMember, RemoveMember
    }

    @JsonProperty("Command")
    private Command command;

    @JsonDeserialize(using = CustomMemberIdDeserializer.class)
    @JsonSerialize(using = CustomMemberIdSerializer.class)
    @JsonProperty("MemberId")
    private MemberId memberId;

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public MemberId getMemberId() {
        return memberId;
    }

    public void setMemberId(MemberId memberId) {
        this.memberId = memberId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((command == null) ? 0 : command.hashCode());
        result = prime * result + ((memberId == null) ? 0 : memberId.hashCode());
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
        RaftProtocolCommand other = (RaftProtocolCommand) obj;
        if (command != other.command)
            return false;
        if (memberId == null) {
            if (other.memberId != null)
                return false;
        } else if (!memberId.equals(other.memberId))
            return false;
        return true;
    }

}

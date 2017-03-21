package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;

import org.junit.Test;

import com.rvprg.raft.protocol.impl.RaftProtocolCommand;
import com.rvprg.raft.protocol.impl.RaftProtocolCommand.Command;
import com.rvprg.raft.protocol.impl.RaftProtocolCommandJsonSerde;
import com.rvprg.raft.transport.MemberId;

public class RaftProtocolCommandTest {
    @Test
    public void testSerializationDeserialization() throws IOException {
        RaftProtocolCommandJsonSerde serde = new RaftProtocolCommandJsonSerde();

        RaftProtocolCommand original = new RaftProtocolCommand();
        original.setCommand(Command.AddMember);
        original.setMemberId(new MemberId("localhost", 1234));

        String serialized = serde.serialize(original);
        RaftProtocolCommand deserialized = serde.deserialize(serialized);

        assertEquals(original, deserialized);
    }

    @Test
    public void testEquals() throws IOException {
        RaftProtocolCommand cmd1 = new RaftProtocolCommand();
        cmd1.setCommand(Command.AddMember);
        cmd1.setMemberId(new MemberId("localhost", 1234));

        RaftProtocolCommand cmd2 = new RaftProtocolCommand();
        cmd2.setCommand(Command.AddMember);
        cmd2.setMemberId(new MemberId("localhost", 1234));

        assertEquals(cmd1, cmd2);
    }

    @Test
    public void testNotEquals() throws IOException {
        RaftProtocolCommand cmd1 = new RaftProtocolCommand();
        cmd1.setCommand(Command.AddMember);
        cmd1.setMemberId(new MemberId("localhost1", 1234));

        RaftProtocolCommand cmd2 = new RaftProtocolCommand();
        cmd2.setCommand(Command.AddMember);
        cmd2.setMemberId(new MemberId("localhost2", 1234));

        assertNotEquals(cmd1, cmd2);
    }
}

package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.rvprg.raft.transport.MemberId;

public class MemberIdTest {
    @Test
    public void testMemberIdEquals_ipv4() {
        MemberId memberId0 = new MemberId("localhost", 12345);
        assertEquals("localhost/127.0.0.1:12345", memberId0.toString());
        MemberId memberId1 = new MemberId("localhost", 12345);
        assertEquals("localhost/127.0.0.1:12345", memberId1.toString());

        assertEquals(memberId0.toString(), memberId1.toString());
        assertTrue(memberId0.equals(memberId1));
    }

    @Test
    public void testMemberIdEquals_ipv6() {
        MemberId memberId0 = new MemberId("0:0:0:0:0:0:0:1", 12345);
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId0.toString());
        MemberId memberId1 = new MemberId("::1", 12345);
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId1.toString());

        assertEquals(memberId0.toString(), memberId1.toString());
        assertTrue(memberId0.equals(memberId1));
    }

    @Test
    public void testMemberIdParsingFromString_ShouldParse() {
        MemberId memberId = MemberId.fromString("localhost:12345");
        assertEquals("localhost/127.0.0.1:12345", memberId.toString());

        memberId = MemberId.fromString("localhost/127.0.0.1:12345");
        assertEquals("localhost/127.0.0.1:12345", memberId.toString());

        memberId = MemberId.fromString("/127.0.0.1:12345");
        assertEquals("/127.0.0.1:12345", memberId.toString());

        memberId = MemberId.fromString("::1:12345");
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId.toString());

        memberId = MemberId.fromString("/0:0:0:0:0:0:0:1:12345");
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId.toString());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testMemberIdParsingFromString_ShouldFail_NoAddress1() {
        MemberId.fromString("localhost/:12345");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMemberIdParsingFromString_ShouldFail_NoAddress2() {
        MemberId.fromString("/:12345");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMemberIdParsingFromString_ShouldFail_NoPort() {
        MemberId.fromString("localhost:");
    }
}

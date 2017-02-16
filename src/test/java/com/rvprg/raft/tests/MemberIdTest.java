package com.rvprg.raft.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.rvprg.raft.transport.MemberId;

public class MemberIdTest {
    @Test
    public void testMemberIdEquals_ipv4() {
        MemberId memberId0 = new MemberId("localhost", 12345);
        assertEquals("localhost/127.0.0.1:12345", memberId0.getId());
        MemberId memberId1 = new MemberId("localhost", 12345);
        assertEquals("localhost/127.0.0.1:12345", memberId1.getId());

        assertEquals(memberId0.getId(), memberId1.getId());
        assertTrue(memberId0.equals(memberId1));
    }

    @Test
    public void testMemberIdEquals_ipv6() {
        MemberId memberId0 = new MemberId("0:0:0:0:0:0:0:1", 12345);
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId0.getId());
        MemberId memberId1 = new MemberId("::1", 12345);
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId1.getId());

        assertEquals(memberId0.getId(), memberId1.getId());
        assertTrue(memberId0.equals(memberId1));
    }

    @Test
    public void testMemberIdParsingFromString_ShouldParse() {
        MemberId memberId = MemberId.getInstanceFromString("localhost:12345");
        assertEquals("localhost/127.0.0.1:12345", memberId.getId());

        memberId = MemberId.getInstanceFromString("localhost/127.0.0.1:12345");
        assertEquals("localhost/127.0.0.1:12345", memberId.getId());

        memberId = MemberId.getInstanceFromString("/127.0.0.1:12345");
        assertEquals("/127.0.0.1:12345", memberId.getId());

        memberId = MemberId.getInstanceFromString("::1:12345");
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId.getId());

        memberId = MemberId.getInstanceFromString("/0:0:0:0:0:0:0:1:12345");
        assertEquals("/0:0:0:0:0:0:0:1:12345", memberId.getId());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testMemberIdParsingFromString_ShouldFail_NoAddress1() {
        MemberId.getInstanceFromString("localhost/:12345");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMemberIdParsingFromString_ShouldFail_NoAddress2() {
        MemberId.getInstanceFromString("/:12345");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMemberIdParsingFromString_ShouldFail_NoPort() {
        MemberId.getInstanceFromString("localhost:");
    }
}

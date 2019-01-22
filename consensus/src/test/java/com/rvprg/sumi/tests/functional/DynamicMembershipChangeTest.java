package com.rvprg.sumi.tests.functional;

import com.rvprg.sumi.log.LogException;
import com.rvprg.sumi.log.SnapshotInstallException;
import com.rvprg.sumi.protocol.*;
import com.rvprg.sumi.tests.helpers.ConsensusFunctionalBase;
import com.rvprg.sumi.tests.helpers.NetworkUtils;
import com.rvprg.sumi.transport.MemberId;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class DynamicMembershipChangeTest extends ConsensusFunctionalBase {

    // This functionality is broken atm.
    @Ignore
    @Test//(timeout = 60000)
    public void testAddMemberDynamically()
            throws NoSuchMethodException, SecurityException, InterruptedException, LogException, SnapshotInstallException, IOException {
        int clusterSize = 5;
        int logSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize, 300, 1000);
        cluster.start();

        for (int i = 0; i < logSize; ++i) {
            byte[] buff = ByteBuffer.allocate(4).putInt(i).array();
            ApplyCommandResult applyCommandResult = cluster.getLeader().applyCommand(buff);
            if (applyCommandResult.getResult() != null) {
                try {
                    applyCommandResult.getResult().get(3000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    // fine
                }
            }
            ;
        }

        cluster.waitUntilCommitAdvances();
        cluster.waitUntilFollowersAdvance();

        MemberId newMemberId = new MemberId("localhost", NetworkUtils.getRandomFreePort());
        Set<MemberId> peers = (new HashSet<>(cluster.getMembers()));
        CountDownLatch newMemberStartLatch = new CountDownLatch(1);
        CountDownLatch newMemberShutdownLatch = new CountDownLatch(1);

        ConsensusEventListener newMemberListener = new ConsensusEventListenerImpl() {
            @Override
            public void started() {
                newMemberStartLatch.countDown();
            }

            @Override
            public void shutdown() {
                newMemberShutdownLatch.countDown();
            }
        };

        try {
            cluster.getLeader().addMemberDynamically(new MemberId("localhost", 12345));
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }

        Consensus newRaftMember = getRaft(newMemberId.getHostName(), newMemberId.getPort(), peers, 300, 1000, newMemberListener);
        newRaftMember.becomeCatchingUpMember();
        newRaftMember.start();

        newMemberStartLatch.await();

        // Phase 1: Add as a catching up server.
        AddCatchingUpMemberResult addCatchingUpMemberResult = cluster.getLeader().addCatchingUpMember(newMemberId);
        assertNotNull(addCatchingUpMemberResult.getResult());
        assertTrue(addCatchingUpMemberResult.getResult());

        // Polling.
        while (true) {
            if (newRaftMember.getLog().getCommitIndex() == cluster.getLeader().getLog().getCommitIndex()) {
                break;
            }
            Thread.sleep(100);
        }

        // Phase 2: Add as a normal member
        ApplyCommandResult addMemberDynamicallyResult = cluster.getLeader().addMemberDynamically(newMemberId);
        newRaftMember.becomeVotingMember();
        assertNotNull(addMemberDynamicallyResult.getResult());
        final AtomicBoolean addSuccess = new AtomicBoolean(false);
        try {
            addSuccess.set(addMemberDynamicallyResult.getResult().get(10000, TimeUnit.MILLISECONDS));
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            addSuccess.set(false);
        }

        cluster.getRafts().add(newRaftMember);
        cluster.waitUntilCommitAdvances();
        cluster.waitUntilFollowersAdvance();

        cluster.shutdown();
        newRaftMember.shutdown();
        newMemberShutdownLatch.await();

        assertTrue(addSuccess.get());

        cluster.checkLastIndexes();
        cluster.checkLogConsistency();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveMemberDynamically_LeaderRemovesItself()
            throws NoSuchMethodException, SecurityException, InterruptedException, IllegalArgumentException,
            SnapshotInstallException, IOException, LogException {
        int clusterSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize - 1, 300, 500);
        cluster.start();
        Consensus currentLeader = cluster.getLeader();

        // Try to remove the leader.
        try {
            currentLeader.removeMemberDynamically(currentLeader.getMemberId());
        } finally {
            cluster.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void testRemoveMemberDynamically()
            throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException,
            SnapshotInstallException, IOException, LogException {
        int clusterSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize - 1, 300, 500);
        cluster.start();
        Consensus memberToRemove = cluster.getLeader();

        memberToRemove.becomeCatchingUpMember();
        while (memberToRemove.getRole() == MemberRole.Leader) {
            Thread.sleep(100);
        }

        Consensus currentLeader = cluster.getLeader();

        // Remove a member.
        ApplyCommandResult removeCatchingUpMemberResult = currentLeader.removeMemberDynamically(memberToRemove.getMemberId());
        assertNotNull(removeCatchingUpMemberResult.getResult());
        assertTrue(removeCatchingUpMemberResult.getResult().get());

        cluster.waitUntilCommitAdvances();
        cluster.getRafts().remove(memberToRemove);
        cluster.waitUntilFollowersAdvance();

        Field memberConnectorField = ConsensusImpl.class.getDeclaredField("memberConnector");
        memberConnectorField.setAccessible(true);

        for (Consensus raft : cluster.getRafts()) {
            ConsensusMemberConnector raftMemberConnector = (ConsensusMemberConnector) memberConnectorField.get(raft);
            // Additional -1 because raftMemberConnector excludes itself.
            assertEquals(clusterSize - 1 - 1, raftMemberConnector.getVotingMembersCount());
        }

        currentLeader.shutdown();
        cluster.shutdown();
    }
}

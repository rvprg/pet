package com.rvprg.sumi.tests.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
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

import org.junit.Test;

import com.rvprg.sumi.log.LogException;
import com.rvprg.sumi.log.SnapshotInstallException;
import com.rvprg.sumi.protocol.AddCatchingUpMemberResult;
import com.rvprg.sumi.protocol.ApplyCommandResult;
import com.rvprg.sumi.protocol.MemberRole;
import com.rvprg.sumi.protocol.Consensus;
import com.rvprg.sumi.protocol.ConsensusImpl;
import com.rvprg.sumi.protocol.ConsensusEventListener;
import com.rvprg.sumi.protocol.ConsensusEventListenerImpl;
import com.rvprg.sumi.protocol.ConsensusMemberConnector;
import com.rvprg.sumi.tests.helpers.NetworkUtils;
import com.rvprg.sumi.tests.helpers.ConsensusFunctionalBase;
import com.rvprg.sumi.transport.MemberId;

public class DynamicMembershipChangeTest extends ConsensusFunctionalBase {

    @Test(timeout = 60000)
    public void testAddMemberDynamically()
            throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException, LogException, FileNotFoundException, SnapshotInstallException, IOException {
        int clusterSize = 5;
        int logSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize, 9000, 10000);
        cluster.start();

        Consensus currentLeader = cluster.getLeader();
        for (int i = 0; i < logSize; ++i) {
            byte[] buff = ByteBuffer.allocate(4).putInt(i).array();
            ApplyCommandResult applyCommandResult = currentLeader.applyCommand(buff);
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
        Set<MemberId> peers = (new HashSet<MemberId>(cluster.getMembers()));
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
            currentLeader.addMemberDynamically(new MemberId("localhost", 12345));
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            // Expected
        }

        Consensus newRaftMember = getRaft(newMemberId.getHostName(), newMemberId.getPort(), peers, 9000, 10000, newMemberListener);
        newRaftMember.becomeCatchingUpMember();
        newRaftMember.start();

        newMemberStartLatch.await();

        // Phase 1: Add as a catching up server.
        AddCatchingUpMemberResult addCatchingUpMemberResult = currentLeader.addCatchingUpMember(newMemberId);
        assertTrue(addCatchingUpMemberResult.getResult() != null);
        assertTrue(addCatchingUpMemberResult.getResult());

        // Polling.
        boolean finished = false;
        while (!finished) {
            finished = true;
            if (newRaftMember.getLog().getCommitIndex() != currentLeader.getLog().getCommitIndex()) {
                finished = false;
                Thread.sleep(100);
                break;
            }
        }

        // Phase 2: Add as a normal member
        ApplyCommandResult addMemberDynamicallyResult = currentLeader.addMemberDynamically(newMemberId);
        newRaftMember.becomeVotingMember();
        assertTrue(addMemberDynamicallyResult.getResult() != null);
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
            throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException,
            FileNotFoundException, SnapshotInstallException, IOException, LogException {
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
            FileNotFoundException, SnapshotInstallException, IOException, LogException {
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
        assertTrue(removeCatchingUpMemberResult.getResult() != null);
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

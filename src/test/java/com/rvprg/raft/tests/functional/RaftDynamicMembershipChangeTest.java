package com.rvprg.raft.tests.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

import com.rvprg.raft.log.LogException;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.impl.AddCatchingUpMemberResult;
import com.rvprg.raft.protocol.impl.ApplyCommandResult;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.impl.RaftMemberConnector;
import com.rvprg.raft.protocol.impl.RaftObserverImpl;
import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.MemberId;

public class RaftDynamicMembershipChangeTest extends RaftFunctionalTestBase {

    @Test(timeout = 60000)
    public void testAddMemberDynamically() throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException, LogException {
        int clusterSize = 5;
        int logSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize, 9000, 10000);
        cluster.start();

        Raft currentLeader = cluster.getLeader();
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

        RaftObserver newMemberObserver = new RaftObserverImpl() {
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

        Raft newRaftMember = getRaft(newMemberId.getHostName(), newMemberId.getPort(), peers, 9000, 10000, newMemberObserver);
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
            throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        int clusterSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize - 1, 300, 500);
        cluster.start();
        Raft currentLeader = cluster.getLeader();

        // Try to remove the leader.
        try {
            currentLeader.removeMemberDynamically(currentLeader.getMemberId());
        } finally {
            cluster.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void testRemoveMemberDynamically()
            throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        int clusterSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, clusterSize, clusterSize - 1, 300, 500);
        cluster.start();
        Raft memberToRemove = cluster.getLeader();

        memberToRemove.becomeCatchingUpMember();
        while (memberToRemove.getRole() == Role.Leader) {
            Thread.sleep(100);
        }

        Raft currentLeader = cluster.getLeader();

        // Remove a member.
        ApplyCommandResult removeCatchingUpMemberResult = currentLeader.removeMemberDynamically(memberToRemove.getMemberId());
        assertTrue(removeCatchingUpMemberResult.getResult() != null);
        assertTrue(removeCatchingUpMemberResult.getResult().get());

        cluster.waitUntilCommitAdvances();
        cluster.getRafts().remove(memberToRemove);
        cluster.waitUntilFollowersAdvance();

        Field memberConnectorField = RaftImpl.class.getDeclaredField("memberConnector");
        memberConnectorField.setAccessible(true);

        for (Raft raft : cluster.getRafts()) {
            RaftMemberConnector raftMemberConnector = (RaftMemberConnector) memberConnectorField.get(raft);
            // Additional -1 because raftMemberConnector excludes itself.
            assertEquals(clusterSize - 1 - 1, raftMemberConnector.getVotingMembersCount());
        }

        currentLeader.shutdown();
        cluster.shutdown();
    }
}

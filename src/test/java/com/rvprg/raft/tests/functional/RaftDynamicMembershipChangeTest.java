package com.rvprg.raft.tests.functional;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.impl.AddCatchingUpMemberResult;
import com.rvprg.raft.protocol.impl.ApplyCommandResult;
import com.rvprg.raft.protocol.impl.RaftObserverImpl;
import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.MemberId;

public class RaftDynamicMembershipChangeTest extends RaftFunctionalTestBase {
    @Test(timeout = 60000)
    public void testAddMemberDynamically() throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException {
        int clusterSize = 5;
        int logSize = 5;

        RaftCluster cluster = new RaftCluster(clusterSize, 9000, 10000);
        cluster.start();

        Raft currentLeader = cluster.getLeader();
        for (int i = 0; i < logSize; ++i) {
            byte[] buff = ByteBuffer.allocate(4).putInt(i).array();
            ApplyCommandResult applyCommandResult = currentLeader.applyCommand(buff);
            applyCommandResult.getResult().ifPresent(x -> {
                try {
                    x.get(3000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    // fine
                }
            });
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

        Raft newRaftMember = getRaft(newMemberId.getHostName(), newMemberId.getPort(), peers, 9000, 10000, newMemberObserver);
        newRaftMember.becomeCatchingUpMember();
        newRaftMember.start();

        newMemberStartLatch.await();

        // Phase 1: Add as a catching up server.
        AddCatchingUpMemberResult addCatchingUpMemberResult = currentLeader.addCatchingUpMember(newMemberId);
        assertTrue(addCatchingUpMemberResult.getResult().isPresent());
        assertTrue(addCatchingUpMemberResult.getResult().get());

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
        assertTrue(addMemberDynamicallyResult.getResult().isPresent());
        final AtomicBoolean addSuccess = new AtomicBoolean(false);
        addMemberDynamicallyResult.getResult().ifPresent(x -> {
            try {
                addSuccess.set(x.get(10000, TimeUnit.MILLISECONDS));
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                addSuccess.set(false);
            }
        });

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
}
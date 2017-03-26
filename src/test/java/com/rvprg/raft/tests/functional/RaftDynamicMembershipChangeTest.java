package com.rvprg.raft.tests.functional;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

public class RaftDynamicMembershipChangeTest {
    @Test(timeout = 60000)
    public void testAddMemberDynamically() throws NoSuchMethodException, SecurityException, InterruptedException, ExecutionException {
        // TODO: check/prevent spontaneous leadership changes?
        // Check logs.
        int clusterSize = 5;
        int logSize = 5;

        List<Raft> rafts = new ArrayList<Raft>();
        Set<MemberId> members = new HashSet<MemberId>();
        for (Integer port : NetworkUtils.getRandomFreePorts(clusterSize)) {
            members.add(new MemberId("localhost", port));
        }

        CountDownLatch startLatch = new CountDownLatch(members.size());
        CountDownLatch shutdownLatch = new CountDownLatch(members.size());

        RaftObserver observer = new RaftObserverImpl() {
            @Override
            public void started() {
                startLatch.countDown();
            }

            @Override
            public void shutdown() {
                shutdownLatch.countDown();
            }
        };

        for (MemberId member : members) {
            Set<MemberId> peers = (new HashSet<MemberId>(members));
            peers.remove(member);
            rafts.add(RaftTestUtils.getRaft(member.getHostName(), member.getPort(), peers, 9000, 10000, observer));
        }

        for (Raft raft : rafts) {
            raft.start();
        }
        startLatch.await();

        Raft currentLeader = RaftTestUtils.getLeader(rafts);
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

        // Polling. Wait until commit index advances.
        while (currentLeader.getLog().getCommitIndex() != currentLeader.getLog().getLastIndex()) {
            Thread.sleep(100);
        }

        // Polling. Wait until all followers advance their commit indexes.
        boolean finished = false;
        while (!finished) {
            finished = true;
            for (Raft raft : rafts) {
                if (raft.getLog().getCommitIndex() != currentLeader.getLog().getCommitIndex()) {
                    finished = false;
                    Thread.sleep(100);
                    break;
                }
            }
        }

        MemberId newMemberId = new MemberId("localhost", NetworkUtils.getRandomFreePort());
        Set<MemberId> peers = (new HashSet<MemberId>(members));
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

        Raft newRaftMember = RaftTestUtils.getRaft(newMemberId.getHostName(), newMemberId.getPort(), peers, 9000, 10000, newMemberObserver);
        newRaftMember.becomeCatchingUpMember();
        newRaftMember.start();

        newMemberStartLatch.await();

        AddCatchingUpMemberResult addCatchingUpMemberResult = currentLeader.addCatchingUpMember(newMemberId);
        assertTrue(addCatchingUpMemberResult.getResult().isPresent());
        assertTrue(addCatchingUpMemberResult.getResult().get());

        // Polling.
        finished = false;
        while (!finished) {
            finished = true;
            if (newRaftMember.getLog().getCommitIndex() != currentLeader.getLog().getCommitIndex()) {
                finished = false;
                Thread.sleep(100);
                break;
            }
        }

        ApplyCommandResult addMemberDynamicallyResult = currentLeader.addMemberDynamically(newMemberId);
        assertTrue(addMemberDynamicallyResult.getResult().isPresent());
        final AtomicBoolean addSuccess = new AtomicBoolean(false);
        addMemberDynamicallyResult.getResult().ifPresent(x -> {
            try {
                addSuccess.set(x.get(10000, TimeUnit.MILLISECONDS));
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                addSuccess.set(false);
            }
        });

        for (Raft raft : rafts) {
            raft.shutdown();
        }

        newRaftMember.shutdown();
        newMemberShutdownLatch.await();

        assertTrue(addSuccess.get());
    }
}

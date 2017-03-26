package com.rvprg.raft.tests.functional;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.impl.ApplyCommandResult;
import com.rvprg.raft.protocol.impl.LogEntry;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.impl.RaftObserverImpl;
import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.MemberId;

public class RaftLogConsistencyTest {

    @Test
    public void testLogConsistencyProperty()
            throws InterruptedException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // This test creates a cluster of clusterSize members. Then it applies
        // applyCount different commands. After that it elects a new leader and
        // starts everything again. It repeats the cycle iterations times.
        // After that it checks that the log consistency property holds.
        int clusterSize = 5;
        int iterations = 5;
        int applyCount = 5;

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

        Method cancelHeartBeat = RaftImpl.class.getDeclaredMethod("becomeFollower", new Class[] {});
        cancelHeartBeat.setAccessible(true);

        for (MemberId member : members) {
            Set<MemberId> peers = (new HashSet<MemberId>(members));
            peers.remove(member);
            rafts.add(RaftTestUtils.getRaft(member.getHostName(), member.getPort(), peers, observer));
        }

        for (Raft raft : rafts) {
            raft.start();
        }
        startLatch.await();

        int commandNumber = 0;
        Raft currentLeader = null;
        for (int i = 0; i < iterations; ++i) {
            currentLeader = RaftTestUtils.getLeader(rafts);
            for (int j = 0; j < applyCount; ++j) {
                commandNumber++;
                byte[] buff = ByteBuffer.allocate(4).putInt(commandNumber).array();
                ApplyCommandResult applyCommandResult = currentLeader.applyCommand(buff);

                applyCommandResult.getResult().ifPresent(x -> {
                    try {
                        x.get(3000, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException | InterruptedException | ExecutionException e) {
                        // fine
                    }
                });
            }

            if (i < iterations - 1) {
                cancelHeartBeat.invoke(currentLeader, new Object[] {});
            }
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

        for (Raft raft : rafts) {
            raft.shutdown();
        }

        shutdownLatch.await();

        checkLastIndexes(rafts);
        checkLogConsistency(rafts);
    }

    private void checkLogConsistency(List<Raft> rafts) {
        Log firstRaftLog = rafts.get(0).getLog();
        int firstIndex = firstRaftLog.getFirstIndex();
        int lastIndex = firstRaftLog.getCommitIndex();

        for (int i = firstIndex; i <= lastIndex; ++i) {
            LogEntry le = firstRaftLog.get(i);
            for (Raft raft : rafts) {
                assertTrue(raft.getLog().get(i).equals(le));
            }
        }
    }

    private void checkLastIndexes(List<Raft> rafts) {
        int lastIndex = rafts.get(0).getLog().getLastIndex();
        assertTrue(rafts.stream().map(x -> x.getLog().getLastIndex()).allMatch((x) -> lastIndex == x));
    }

}

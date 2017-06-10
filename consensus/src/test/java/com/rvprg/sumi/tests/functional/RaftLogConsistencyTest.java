package com.rvprg.sumi.tests.functional;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.rvprg.sumi.log.LogException;
import com.rvprg.sumi.log.SnapshotInstallException;
import com.rvprg.sumi.protocol.ApplyCommandResult;
import com.rvprg.sumi.protocol.Raft;
import com.rvprg.sumi.protocol.RaftImpl;
import com.rvprg.sumi.tests.helpers.RaftFunctionalBase;

public class RaftLogConsistencyTest extends RaftFunctionalBase {

    @Test
    public void testLogConsistencyProperty()
            throws InterruptedException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, LogException,
            FileNotFoundException, SnapshotInstallException, IOException {
        // This test creates a cluster of clusterSize members. Then it applies
        // applyCount different commands. After that it elects a new leader and
        // starts everything again. It repeats the cycle iterations times.
        // After that it checks that the log consistency property holds.
        int clusterSize = 5;
        int iterations = 5;
        int applyCount = 5;

        RaftCluster cluster = new RaftCluster(clusterSize);
        Method cancelHeartBeat = RaftImpl.class.getDeclaredMethod("becomeFollower", new Class[] {});
        cancelHeartBeat.setAccessible(true);
        cluster.start();

        int commandNumber = 0;
        Raft currentLeader = null;
        for (int i = 0; i < iterations; ++i) {
            currentLeader = cluster.getLeader();
            for (int j = 0; j < applyCount; ++j) {
                commandNumber++;
                byte[] buff = ByteBuffer.allocate(4).putInt(commandNumber).array();
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

            if (i < iterations - 1) {
                cancelHeartBeat.invoke(currentLeader, new Object[] {});
            }
        }

        cluster.waitUntilCommitAdvances();
        cluster.waitUntilFollowersAdvance();

        cluster.shutdown();

        cluster.checkLastIndexes();
        cluster.checkLogConsistency();
    }

}

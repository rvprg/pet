package com.rvprg.raft.tests.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.LogEntry;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.impl.TransientLogImpl;
import com.rvprg.raft.sm.Command;
import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageReceiver;

public class RaftElectionTest {

    private Raft getRaft(String host, int port, Set<MemberId> nodes, RaftObserver raftObserver) {
        Configuration configuration = Configuration.newBuilder().memberId(new MemberId(host, port)).addMemberIds(nodes).build();

        Injector injector = Guice.createInjector(new Module(configuration));
        MemberConnector memberConnector = injector.getInstance(MemberConnector.class);

        MessageReceiver messageReceiver = injector.getInstance(MessageReceiver.class);
        // Fake log entries.
        Log log = new TransientLogImpl();
        log.add(0, new LogEntry() {
            @Override
            public int getTerm() {
                return 0;
            }

            @Override
            public Command getCommand() {
                return null;
            }
        });

        return new RaftImpl(configuration, memberConnector, messageReceiver, log, raftObserver);
    }

    @Test
    public void testRandomCrashes() throws InterruptedException {
        // This test creates a cluster containing clusterSize members. It then
        // starts all of them, and waits until a leader is elected. Then it
        // kills the leader and restarts it. A new leader is elected. It
        // then repeats this for a number of times specified by iteration. Then
        // it checks if terms were growing monotonically and that in each
        // term there was no more than one leader elected.
        int clusterSize = 5;
        int iterations = 10;

        // Used to phase election iterations.
        Phaser phaser = new Phaser() {
            @Override
            protected boolean onAdvance(int phase, int parties) {
                return false;
            }
        };

        List<Raft> rafts = new ArrayList<Raft>();
        Set<MemberId> members = new HashSet<MemberId>();
        for (Integer port : NetworkUtils.getRandomFreePorts(clusterSize)) {
            members.add(new MemberId("localhost", port));
        }

        CountDownLatch startLatch = new CountDownLatch(members.size());
        LinkedHashMap<Integer, Integer> termsAndLeaders = new LinkedHashMap<>();

        RaftObserver observer = new RaftObserver() {

            @Override
            public void voteRejected() {
            }

            @Override
            public void voteReceived() {
            }

            @Override
            public void nextElectionScheduled() {
            }

            @Override
            public void heartbeatTimedout() {
            }

            @Override
            public void heartbeatReceived() {
            }

            @Override
            public synchronized void electionWon(int term) {
                if (termsAndLeaders.containsKey(term)) {
                    termsAndLeaders.put(term, termsAndLeaders.get(term) + 1);
                } else {
                    termsAndLeaders.put(term, 1);
                }
                phaser.arrive();
            }

            @Override
            public void electionTimedout() {
            }

            @Override
            public void started() {
                startLatch.countDown();
            }

            @Override
            public void shutdown() {
            }
        };

        for (MemberId member : members) {
            Set<MemberId> peers = (new HashSet<MemberId>(members));
            peers.remove(member);
            rafts.add(getRaft(member.getHostName(), member.getPort(), peers, observer));
        }

        for (Raft raft : rafts) {
            raft.start();
        }

        startLatch.await();

        phaser.register();
        for (int i = 0; i < iterations; ++i) {
            phaser.awaitAdvance(i);

            Raft currentLeader = null;
            for (Raft raft : rafts) {
                if (raft.getRole() == Role.Leader) {
                    raft.shutdown();
                    currentLeader = raft;
                    break;
                }
            }

            rafts.remove(currentLeader);
            Configuration c = currentLeader.getConfiguration();
            MemberId member = currentLeader.getId();
            Raft newRaft = getRaft(member.getHostName(), member.getPort(), c.getMemberIds(), observer);
            newRaft.start();
            rafts.add(newRaft);
        }

        for (Raft raft : rafts) {
            raft.shutdown();
        }

        Integer prevTerm = 0;
        for (Integer term : termsAndLeaders.keySet()) {
            // Check terms are monotonically increasing
            assertTrue(prevTerm < term);
            // Always only one leader in each term
            assertEquals(1, termsAndLeaders.get(term).intValue());
            prevTerm = term;
        }
    }

}

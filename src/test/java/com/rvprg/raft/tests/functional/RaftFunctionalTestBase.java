package com.rvprg.raft.tests.functional;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.Role;
import com.rvprg.raft.protocol.impl.LogEntry;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.impl.RaftObserverImpl;
import com.rvprg.raft.protocol.impl.TransientLogImpl;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageReceiver;

public class RaftFunctionalTestBase {
    public Raft getRaft(String host, int port, Set<MemberId> nodes, RaftObserver raftObserver) {
        return getRaft(host, port, nodes, 150, 300, raftObserver);
    }

    public Raft getRaft(String host, int port, Set<MemberId> nodes, int electionMinTimeout, int electionMaxTimeout, RaftObserver raftObserver) {
        Configuration configuration = Configuration.newBuilder().memberId(new MemberId(host, port)).addMemberIds(nodes).electionMinTimeout(electionMinTimeout)
                .electionMaxTimeout(electionMaxTimeout).build();

        Injector injector = Guice.createInjector(new Module(configuration));
        MemberConnector memberConnector = injector.getInstance(MemberConnector.class);
        StateMachine stateMachine = injector.getInstance(StateMachine.class);
        MessageReceiver messageReceiver = injector.getInstance(MessageReceiver.class);

        Log log = new TransientLogImpl();
        return new RaftImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftObserver);
    }

    public class RaftCluster {
        private final List<Raft> rafts;

        private final CountDownLatch startLatch;
        private final CountDownLatch shutdownLatch;
        private final RaftObserver observer;
        private final Set<MemberId> members;

        private final Method cancelHeartBeat;

        public Set<MemberId> getMembers() {
            return ImmutableSet.copyOf(members);
        }

        public RaftCluster(int clusterSize) throws NoSuchMethodException, SecurityException {
            this(clusterSize, 250, 300);
        }

        public RaftCluster(int clusterSize, int electionMinTimeout, int electionMaxTimeout) throws NoSuchMethodException, SecurityException {
            startLatch = new CountDownLatch(clusterSize);
            shutdownLatch = new CountDownLatch(clusterSize);

            observer = new RaftObserverImpl() {
                @Override
                public void started() {
                    startLatch.countDown();
                }

                @Override
                public void shutdown() {
                    shutdownLatch.countDown();
                }
            };

            members = createMembers(clusterSize);
            rafts = createRafts(members, electionMinTimeout, electionMaxTimeout, observer);

            cancelHeartBeat = RaftImpl.class.getDeclaredMethod("cancelPeriodicHeartbeatTask", new Class[] {});
            cancelHeartBeat.setAccessible(true);
        }

        public RaftCluster(int clusterSize, final RaftObserver observer) throws NoSuchMethodException, SecurityException {
            this(clusterSize, 250, 300, observer);
        }

        public RaftCluster(int clusterSize, int electionMinTimeout, int electionMaxTimeout, final RaftObserver observer) throws NoSuchMethodException, SecurityException {
            startLatch = new CountDownLatch(clusterSize);
            shutdownLatch = new CountDownLatch(clusterSize);

            cancelHeartBeat = RaftImpl.class.getDeclaredMethod("cancelPeriodicHeartbeatTask", new Class[] {});
            cancelHeartBeat.setAccessible(true);

            this.observer = new RaftObserver() {
                @Override
                public void started() {
                    startLatch.countDown();
                    observer.started();
                }

                @Override
                public void shutdown() {
                    shutdownLatch.countDown();
                    observer.shutdown();
                }

                @Override
                public void heartbeatTimedout() {
                    observer.heartbeatTimedout();
                }

                @Override
                public void nextElectionScheduled() {
                    observer.nextElectionScheduled();
                }

                @Override
                public void heartbeatReceived() {
                    observer.heartbeatReceived();
                }

                @Override
                public void voteReceived() {
                    observer.voteReceived();
                }

                @Override
                public void voteRejected() {
                    observer.voteRejected();
                }

                @Override
                public void electionWon(int term, Raft leader) {
                    observer.electionWon(term, leader);
                }

                @Override
                public void electionTimedout() {
                    observer.electionTimedout();
                }

                @Override
                public void appendEntriesRetryScheduled(MemberId memberId) {
                    observer.appendEntriesRetryScheduled(memberId);
                }
            };

            members = createMembers(clusterSize);
            rafts = createRafts(members, electionMinTimeout, electionMaxTimeout, this.observer);

        }

        public void start() throws InterruptedException {
            for (Raft raft : rafts) {
                raft.start();
            }
            startLatch.await();
        }

        public void shutdown() throws InterruptedException {
            for (Raft raft : rafts) {
                raft.shutdown();
            }
            shutdownLatch.await();
        }

        public Raft getLeader() {
            do {
                for (Raft raft : rafts) {
                    if (raft.getRole() == Role.Leader) {
                        return raft;
                    }
                }
            } while (true);
        }

        public void waitUntilCommitAdvances() throws InterruptedException {
            Raft currentLeader = getLeader();
            // Polling. Wait until commit index advances.
            while (currentLeader.getLog().getCommitIndex() != currentLeader.getLog().getLastIndex()) {
                Thread.sleep(500);
            }
        }

        public void waitUntilFollowersAdvance() throws InterruptedException {
            Raft currentLeader = getLeader();
            // Polling. Wait until all followers advance their commit indexes.
            boolean finished = false;
            while (!finished) {
                finished = true;
                for (Raft raft : rafts) {
                    if (raft.getLog().getCommitIndex() != currentLeader.getLog().getCommitIndex()) {
                        finished = false;
                        Thread.sleep(500);
                        break;
                    }
                }
            }
        }

        public void checkLogConsistency() {
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

        public void checkLastIndexes() {
            int lastIndex = rafts.get(0).getLog().getLastIndex();
            assertTrue(rafts.stream().map(x -> x.getLog().getLastIndex()).allMatch((x) -> lastIndex == x));
        }

        public List<Raft> getRafts() {
            return rafts;
        }

        public Set<MemberId> createMembers(int clusterSize) {
            Set<MemberId> members = new HashSet<MemberId>();
            for (Integer port : NetworkUtils.getRandomFreePorts(clusterSize)) {
                members.add(new MemberId("localhost", port));
            }
            return members;
        }

        public List<Raft> createRafts(Set<MemberId> members, int electionMinTimeout, int electionMaxTimeout, RaftObserver observer) {
            List<Raft> rafts = new ArrayList<Raft>();
            for (MemberId member : members) {
                Set<MemberId> peers = (new HashSet<MemberId>(members));
                peers.remove(member);
                rafts.add(getRaft(member.getHostName(), member.getPort(), peers, electionMinTimeout, electionMaxTimeout, observer));
            }
            return rafts;
        }

        public void cancelElection(Raft raft) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            cancelHeartBeat.invoke(raft, new Object[] {});
        }

        public void cancelElectionForAll() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            for (Raft raft : rafts) {
                cancelElection(raft);
            }
        }

    }

}

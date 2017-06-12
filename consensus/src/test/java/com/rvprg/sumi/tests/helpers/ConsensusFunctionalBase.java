package com.rvprg.sumi.tests.helpers;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.LogEntry;
import com.rvprg.sumi.Module;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.log.Log;
import com.rvprg.sumi.log.LogException;
import com.rvprg.sumi.log.SnapshotInstallException;
import com.rvprg.sumi.protocol.MemberRole;
import com.rvprg.sumi.protocol.Consensus;
import com.rvprg.sumi.protocol.ConsensusImpl;
import com.rvprg.sumi.protocol.ConsensusEventListener;
import com.rvprg.sumi.protocol.ConsensusEventListenerImpl;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.transport.MemberConnector;
import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.MessageReceiver;
import com.rvprg.sumi.transport.SnapshotDescriptor;

public abstract class ConsensusFunctionalBase {
    public Consensus getRaft(String host, int port, Set<MemberId> nodes, ConsensusEventListener raftListener)
            throws InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
        return getRaft(host, port, nodes, 150, 300, raftListener);
    }

    public Consensus getRaft(String host, int port, Set<MemberId> nodes, int electionMinTimeout, int electionMaxTimeout, ConsensusEventListener raftListener)
            throws InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
        File tempDir = Files.createTempDir();
        File snapshotFolderPath = Files.createTempDir();
        Configuration configuration = Configuration.newBuilder()
                .selfId(new MemberId(host, port))
                .addMemberIds(nodes)
                .electionMinTimeout(electionMinTimeout)
                .electionMaxTimeout(electionMaxTimeout)
                .logUri(tempDir.toURI())
                .snapshotFolderPath(snapshotFolderPath)
                .snapshotSenderPort(NetworkUtils.getRandomFreePort())
                .build();

        Injector injector = Guice.createInjector(Modules.override(new Module(configuration)).with(new TestModule()));
        MemberConnector memberConnector = injector.getInstance(MemberConnector.class);
        StateMachine stateMachine = injector.getInstance(StateMachine.class);
        MessageReceiver messageReceiver = injector.getInstance(MessageReceiver.class);

        Log log = injector.getInstance(Log.class);
        return new ConsensusImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftListener);
    }

    public class RaftCluster {
        private final List<Consensus> rafts;

        private final CountDownLatch startLatch;
        private final CountDownLatch shutdownLatch;
        private final ConsensusEventListener listener;
        private final Set<MemberId> members;

        private final Method cancelHeartBeat;

        public Set<MemberId> getMembers() {
            return ImmutableSet.copyOf(members);
        }

        public RaftCluster(int clusterSize)
                throws NoSuchMethodException, SecurityException, InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
            this(clusterSize, clusterSize, clusterSize, 250, 300);
        }

        public RaftCluster(int clusterSize, int startCountDownLatchCount, int shutdownCountDownLatchCount, int electionMinTimeout, int electionMaxTimeout)
                throws NoSuchMethodException, SecurityException, InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
            startLatch = new CountDownLatch(startCountDownLatchCount);
            shutdownLatch = new CountDownLatch(shutdownCountDownLatchCount);

            listener = new ConsensusEventListenerImpl() {
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
            rafts = createRafts(members, electionMinTimeout, electionMaxTimeout, listener);

            cancelHeartBeat = ConsensusImpl.class.getDeclaredMethod("cancelPeriodicHeartbeatTask", new Class[] {});
            cancelHeartBeat.setAccessible(true);
        }

        public RaftCluster(int clusterSize, final ConsensusEventListener listener)
                throws NoSuchMethodException, SecurityException, InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
            this(clusterSize, 250, 300, listener);
        }

        public RaftCluster(int clusterSize, int electionMinTimeout, int electionMaxTimeout, final ConsensusEventListener listener)
                throws NoSuchMethodException, SecurityException, InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
            this(clusterSize, clusterSize, clusterSize, electionMinTimeout, electionMaxTimeout, listener);
        }

        public RaftCluster(int clusterSize, int startCountDownLatchCount, int shutdownCountDownLatchCount, int electionMinTimeout, int electionMaxTimeout,
                final ConsensusEventListener listener)
                throws NoSuchMethodException, SecurityException, InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
            startLatch = new CountDownLatch(startCountDownLatchCount);
            shutdownLatch = new CountDownLatch(shutdownCountDownLatchCount);

            cancelHeartBeat = ConsensusImpl.class.getDeclaredMethod("cancelPeriodicHeartbeatTask", new Class[] {});
            cancelHeartBeat.setAccessible(true);

            this.listener = new ConsensusEventListener() {
                @Override
                public void started() {
                    startLatch.countDown();
                    listener.started();
                }

                @Override
                public void shutdown() {
                    shutdownLatch.countDown();
                    listener.shutdown();
                }

                @Override
                public void heartbeatTimedout() {
                    listener.heartbeatTimedout();
                }

                @Override
                public void nextElectionScheduled() {
                    listener.nextElectionScheduled();
                }

                @Override
                public void heartbeatReceived() {
                    listener.heartbeatReceived();
                }

                @Override
                public void voteReceived() {
                    listener.voteReceived();
                }

                @Override
                public void voteRejected() {
                    listener.voteRejected();
                }

                @Override
                public void electionWon(int term, Consensus leader) {
                    listener.electionWon(term, leader);
                }

                @Override
                public void electionTimedout() {
                    listener.electionTimedout();
                }

                @Override
                public void appendEntriesRetryScheduled(MemberId memberId) {
                    listener.appendEntriesRetryScheduled(memberId);
                }

                @Override
                public void snapshotInstalled(SnapshotDescriptor descriptor) {

                }
            };

            members = createMembers(clusterSize);
            rafts = createRafts(members, electionMinTimeout, electionMaxTimeout, this.listener);
        }

        public void start() throws InterruptedException {
            for (Consensus raft : rafts) {
                raft.start();
            }
            startLatch.await();
        }

        public void shutdown() throws InterruptedException {
            for (Consensus raft : rafts) {
                raft.shutdown();
            }
            shutdownLatch.await();
        }

        public Consensus getLeader() {
            do {
                for (Consensus raft : rafts) {
                    if (raft.getRole() == MemberRole.Leader) {
                        return raft;
                    }
                }
            } while (true);
        }

        public void waitUntilCommitAdvances() throws InterruptedException {
            // Polling. Wait until commit index advances.
            Consensus currentLeader = getLeader();
            while (currentLeader.getLog().getCommitIndex() != currentLeader.getLog().getLastIndex()) {
                Thread.sleep(500);
                currentLeader = getLeader();
            }
        }

        public void waitUntilFollowersAdvance() throws InterruptedException {
            // Polling. Wait until all followers advance their commit indexes.
            boolean finished = false;
            while (!finished) {
                Consensus currentLeader = getLeader();
                finished = true;
                for (Consensus raft : rafts) {
                    if (raft.getLog().getCommitIndex() != currentLeader.getLog().getCommitIndex()) {
                        finished = false;
                        Thread.sleep(500);
                        break;
                    }
                }
            }
        }

        public void checkLogConsistency() throws LogException {
            Log firstRaftLog = rafts.get(0).getLog();
            long firstIndex = firstRaftLog.getFirstIndex();
            long lastIndex = firstRaftLog.getCommitIndex();

            for (long i = firstIndex; i <= lastIndex; ++i) {
                LogEntry le = firstRaftLog.get(i);
                for (Consensus raft : rafts) {
                    assertTrue(raft.getLog().get(i).getTerm() == le.getTerm());
                }
            }
        }

        public void checkLastIndexes() {
            long lastIndex = rafts.get(0).getLog().getLastIndex();
            assertTrue(rafts.stream().map(x -> x.getLog().getLastIndex()).allMatch((x) -> lastIndex == x));
        }

        public void checkFirstIndexes() {
            long firstIndex = rafts.get(0).getLog().getFirstIndex();
            assertTrue(rafts.stream().map(x -> x.getLog().getFirstIndex()).allMatch((x) -> firstIndex == x));
        }

        public List<Consensus> getRafts() {
            return rafts;
        }

        public Set<MemberId> createMembers(int clusterSize) {
            Set<MemberId> members = new HashSet<MemberId>();
            for (Integer port : NetworkUtils.getRandomFreePorts(clusterSize)) {
                members.add(new MemberId("localhost", port));
            }
            return members;
        }

        public List<Consensus> createRafts(Set<MemberId> members, int electionMinTimeout, int electionMaxTimeout, ConsensusEventListener listener)
                throws InterruptedException, FileNotFoundException, SnapshotInstallException, IOException, LogException {
            List<Consensus> rafts = new ArrayList<Consensus>();
            for (MemberId member : members) {
                Set<MemberId> peers = (new HashSet<MemberId>(members));
                peers.remove(member);
                rafts.add(getRaft(member.getHostName(), member.getPort(), peers, electionMinTimeout, electionMaxTimeout, listener));
            }
            return rafts;
        }

        public void cancelElection(Consensus raft) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            cancelHeartBeat.invoke(raft, new Object[] {});
        }

        public void cancelElectionForAll() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            for (Consensus raft : rafts) {
                cancelElection(raft);
            }
        }

    }

}

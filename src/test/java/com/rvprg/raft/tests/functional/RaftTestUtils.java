package com.rvprg.raft.tests.functional;

import java.util.Set;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.Module;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Log;
import com.rvprg.raft.protocol.Raft;
import com.rvprg.raft.protocol.RaftObserver;
import com.rvprg.raft.protocol.impl.RaftImpl;
import com.rvprg.raft.protocol.impl.TransientLogImpl;
import com.rvprg.raft.sm.StateMachine;
import com.rvprg.raft.transport.MemberConnector;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MessageReceiver;

public class RaftTestUtils {
    public static Raft getRaft(String host, int port, Set<MemberId> nodes, RaftObserver raftObserver) {
        Configuration configuration = Configuration.newBuilder().memberId(new MemberId(host, port)).addMemberIds(nodes).build();

        Injector injector = Guice.createInjector(new Module(configuration));
        MemberConnector memberConnector = injector.getInstance(MemberConnector.class);
        StateMachine stateMachine = injector.getInstance(StateMachine.class);
        MessageReceiver messageReceiver = injector.getInstance(MessageReceiver.class);

        Log log = new TransientLogImpl();
        return new RaftImpl(configuration, memberConnector, messageReceiver, log, stateMachine, raftObserver);
    }
}

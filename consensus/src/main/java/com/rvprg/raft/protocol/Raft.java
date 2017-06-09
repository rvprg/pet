package com.rvprg.raft.protocol;

import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.log.Log;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.MemberIdentifiable;

public interface Raft extends MessageConsumer, MemberIdentifiable {
    void start() throws InterruptedException;

    void shutdown() throws InterruptedException;

    boolean isStarted();

    int getCurrentTerm();

    MemberRole getRole();

    Configuration getConfiguration();

    Log getLog();

    ApplyCommandResult applyCommand(byte[] command);

    ApplyCommandResult addMemberDynamically(MemberId memberId);

    ApplyCommandResult removeMemberDynamically(MemberId memberId);

    AddCatchingUpMemberResult addCatchingUpMember(MemberId memberId);

    RemoveCatchingUpMemberResult removeCatchingUpMember(MemberId memberId);

    boolean isVotingMember();

    boolean isCatchingUpMember();

    void becomeCatchingUpMember();

    void becomeVotingMember();
}

package com.rvprg.sumi.protocol;

import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.log.Log;
import com.rvprg.sumi.transport.MemberId;
import com.rvprg.sumi.transport.MemberIdentifiable;

public interface Consensus extends MessageConsumer, MemberIdentifiable {
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

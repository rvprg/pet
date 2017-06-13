package com.rvprg.sumi.protocol;

import com.rvprg.sumi.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.sumi.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.sumi.transport.ActiveMember;

public interface MessageConsumer {
    void consumeRequestVote(ActiveMember member, RequestVote requestVote);

    void consumeRequestVoteResponse(ActiveMember member, RequestVoteResponse requestVote);

    void consumeAppendEntries(ActiveMember member, AppendEntries appendEntries);

    void consumeAppendEntriesResponse(ActiveMember member, AppendEntriesResponse appendEntriesResponse);
}

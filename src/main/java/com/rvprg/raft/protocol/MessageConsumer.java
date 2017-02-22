package com.rvprg.raft.protocol;

import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.raft.transport.Member;
import com.rvprg.raft.transport.MemberIdentifiable;

public interface MessageConsumer extends MemberIdentifiable {
    void consumeRequestVote(Member member, RequestVote requestVote);

    void consumeRequestVoteResponse(Member member, RequestVoteResponse requestVote);

    void consumeAppendEntries(Member member, AppendEntries appendEntries);

    void consumeAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse);
}

package com.rvprg.sumi.protocol;

import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;
import com.rvprg.sumi.transport.Member;

public interface MessageConsumer {
    void consumeRequestVote(Member member, RequestVote requestVote);

    void consumeRequestVoteResponse(Member member, RequestVoteResponse requestVote);

    void consumeAppendEntries(Member member, AppendEntries appendEntries);

    void consumeAppendEntriesResponse(Member member, AppendEntriesResponse appendEntriesResponse);
}

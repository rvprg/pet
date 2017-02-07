package com.rvprg.raft.protocol;

import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;

import io.netty.channel.Channel;

public interface MessageConsumer {
    void consumeRequestVote(Channel senderChannel, RequestVote requestVote);

    void consumeRequestVoteResponse(Channel senderChannel, RequestVoteResponse requestVote);

    void consumeAppendEntries(Channel senderChannel, AppendEntries appendEntries);

    void consumeAppendEntriesResponse(Channel senderChannel, AppendEntriesResponse appendEntriesResponse);
}

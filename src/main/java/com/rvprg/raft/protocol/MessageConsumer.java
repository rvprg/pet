package com.rvprg.raft.protocol;

import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntries;
import com.rvprg.raft.protocol.messages.ProtocolMessages.AppendEntriesResponse;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVote;
import com.rvprg.raft.protocol.messages.ProtocolMessages.RequestVoteResponse;

import io.netty.channel.Channel;

public interface MessageConsumer {
    void consumeRequestVote(Channel senderChannel, RequestVote requestVoteMessage);

    void consumeRequestVoteResponse(Channel senderChannel, RequestVoteResponse requestVoteMessage);

    void consumeAppendEntries(Channel senderChannel, AppendEntries appendEntriesMessage);

    void consumeAppendEntriesResponse(Channel senderChannel, AppendEntriesResponse appendEntriesResponseMessage);
}

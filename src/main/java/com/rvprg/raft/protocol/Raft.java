package com.rvprg.raft.protocol;

import java.nio.ByteBuffer;

import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.impl.ApplyCommandResult;

public interface Raft extends MessageConsumer {
    public void start() throws InterruptedException;

    public void shutdown() throws InterruptedException;

    boolean isStarted();

    public int getCurrentTerm();

    public Role getRole();

    public Configuration getConfiguration();

    ApplyCommandResult applyCommand(ByteBuffer command);

    public Log getLog();
}

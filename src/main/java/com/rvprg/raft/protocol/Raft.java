package com.rvprg.raft.protocol;

import com.rvprg.raft.configuration.Configuration;

public interface Raft extends MessageConsumer {
    public void start() throws InterruptedException;

    public void shutdown() throws InterruptedException;

    boolean isStarted();

    public int getCurrentTerm();

    public Role getRole();

    public Configuration getConfiguration();

}

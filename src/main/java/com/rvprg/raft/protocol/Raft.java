package com.rvprg.raft.protocol;

public interface Raft extends MessageConsumer {
    public void start() throws InterruptedException;

    public void shutdown();

}

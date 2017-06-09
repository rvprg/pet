package com.rvprg.raft;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.raft.configuration.Configuration;
import com.rvprg.raft.protocol.Raft;

public class Factory {
    public static Raft getInstance(Configuration configuration) {
        Injector injector = Guice.createInjector(new Module(configuration));
        return injector.getInstance(Raft.class);
    }
}

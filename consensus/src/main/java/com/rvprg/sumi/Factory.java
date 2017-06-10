package com.rvprg.sumi;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rvprg.sumi.configuration.Configuration;
import com.rvprg.sumi.protocol.Raft;

public class Factory {
    public static Raft getInstance(Configuration configuration) {
        Injector injector = Guice.createInjector(new Module(configuration));
        return injector.getInstance(Raft.class);
    }
}

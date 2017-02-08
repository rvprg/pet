package com.rvprg.raft.transport;

import java.net.InetSocketAddress;

public class MemberId extends InetSocketAddress implements Identifiable {
    private static final long serialVersionUID = -8840089459612603760L;
    private final String id;

    public MemberId(InetSocketAddress s) {
        super(s.getAddress(), s.getPort());
        id = s.getAddress() + ":" + s.getPort();
    }

    public MemberId(String hostname, int port) {
        super(hostname, port);
        id = hostname + ":" + port;
    }

    @Override
    public String getId() {
        return id;
    }
}

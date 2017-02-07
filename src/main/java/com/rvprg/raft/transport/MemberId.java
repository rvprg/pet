package com.rvprg.raft.transport;

import java.net.InetSocketAddress;

public class MemberId extends InetSocketAddress {
    private static final long serialVersionUID = -8840089459612603760L;

    public MemberId(InetSocketAddress s) {
        super(s.getHostName(), s.getPort());
    }

    public MemberId(String hostname, int port) {
        super(hostname, port);
    }

    public boolean isSameAs(String candidateId) {
        // FIXME:
        return getAddress().getHostAddress().equalsIgnoreCase(candidateId);
    }
}

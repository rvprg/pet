package com.rvprg.raft.transport;

import java.net.InetSocketAddress;

public class MemberId extends InetSocketAddress {
    private static final long serialVersionUID = -8840089459612603760L;

    public MemberId(InetSocketAddress s) {
        super(s.getAddress(), s.getPort());
    }

    public MemberId(String ip, int port) {
        super(ip, port);
    }

    public static MemberId fromString(String memberId) {
        int pos = memberId.lastIndexOf(":");
        if (pos == -1 || pos + 1 >= memberId.length()) {
            throw new IllegalArgumentException();
        }

        int port = 0;
        try {
            port = Integer.parseInt(memberId.substring(pos + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException();
        }

        String hostStr = memberId.substring(0, pos).trim();
        String ip = hostStr;
        if (hostStr.isEmpty()) {
            throw new IllegalArgumentException();
        }

        pos = hostStr.lastIndexOf("/");
        if (pos == hostStr.length() - 1) {
            throw new IllegalArgumentException();
        }

        String host = hostStr;
        if (pos > -1) {
            host = hostStr.substring(0, pos);
            ip = hostStr.substring(pos + 1);
        }

        return new MemberId(!host.isEmpty() ? host : ip, port);
    }
}

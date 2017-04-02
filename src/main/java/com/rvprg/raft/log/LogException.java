package com.rvprg.raft.log;

public class LogException extends Exception {
    private static final long serialVersionUID = 1L;

    public LogException(Throwable cause) {
        super(cause);
    }

    public LogException() {
    }
}

package com.rvprg.sumi.log;

public class LogException extends Exception {
    private static final long serialVersionUID = 1L;

    public LogException(Throwable cause) {
        super(cause);
    }

    public LogException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogException() {
    }

    public LogException(String string) {
        super(string);
    }
}

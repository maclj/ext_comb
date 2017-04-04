package com.hadoop.plat.log;

/**
 * 标识日志格式解析错误
 */
public class LogParsingException extends RuntimeException {

    private static final long serialVersionUID = 1317606583082739826L;

    public LogParsingException() {
    }

    public LogParsingException(String message) {
        super(message);
    }

    public LogParsingException(Throwable cause) {
        super(cause);
    }

    public LogParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogParsingException(String message,
                                   Throwable cause,
                                   boolean enableSuppression,
                                   boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

package com.sfkg.timeseries.common;

/** Marks a connection, timeout, or transient downstream failure as retryable. */
public class RetryableDownstreamException extends RuntimeException {

    public RetryableDownstreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableDownstreamException(String message) {
        super(message);
    }
}

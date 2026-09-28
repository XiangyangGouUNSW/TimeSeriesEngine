package com.sfkg.timeseries.common;

/** Indicates a downstream call failed for a non-retryable reason. */
public class DownstreamCallException extends RuntimeException {

    public DownstreamCallException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.sfkg.timeseries.common;

/** Raised only after every retryable downstream attempt has failed. */
public class DownstreamUnavailableException extends RuntimeException {

    public DownstreamUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

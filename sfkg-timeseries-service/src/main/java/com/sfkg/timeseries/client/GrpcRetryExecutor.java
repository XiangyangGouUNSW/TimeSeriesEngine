package com.sfkg.timeseries.client;

import com.sfkg.timeseries.common.DownstreamUnavailableException;
import com.sfkg.timeseries.common.DownstreamCallException;
import com.sfkg.timeseries.common.RetryableDownstreamException;
import com.sfkg.timeseries.config.RetryPolicyProperties;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Executes retryable gRPC and HTTP operations with the shared 10/20/40-second policy. */
@Component
public class GrpcRetryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcRetryExecutor.class);

    private final RetryPolicyProperties properties;

    public GrpcRetryExecutor(RetryPolicyProperties properties) {
        this.properties = properties;
    }

    public <T> T execute(String downstream, String operation, Callable<T> action) {
        Throwable lastFailure = null;
        int retries = properties.getBackoffSeconds().size();
        for (int attempt = 0; attempt <= retries; attempt++) {
            try {
                return action.call();
            } catch (Throwable failure) {
                if (!isRetryable(failure)) {
                    throw propagate(failure);
                }
                lastFailure = failure;
                if (attempt == retries) {
                    break;
                }
                long delaySeconds = Math.max(0L, properties.getBackoffSeconds().get(attempt));
                LOG.warn("{} {} failed (attempt {}/{}); retrying in {}s: {}",
                        downstream, operation, attempt + 1, retries + 1, delaySeconds, failure.getMessage());
                waitBeforeRetry(delaySeconds);
            }
        }
        throw new DownstreamUnavailableException(
                downstream + " unavailable or timed out after retries: " + operation, lastFailure);
    }

    private void waitBeforeRetry(long delaySeconds) {
        try {
            TimeUnit.SECONDS.sleep(delaySeconds);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new DownstreamUnavailableException("downstream retry interrupted", exception);
        }
    }

    private boolean isRetryable(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof RetryableDownstreamException
                    || current instanceof ConnectException
                    || current instanceof SocketTimeoutException
                    || current instanceof HttpTimeoutException) {
                return true;
            }
            if (current instanceof StatusRuntimeException statusException) {
                Status.Code code = statusException.getStatus().getCode();
                return code == Status.Code.UNAVAILABLE
                        || code == Status.Code.DEADLINE_EXCEEDED
                        || code == Status.Code.RESOURCE_EXHAUSTED;
            }
            if (current instanceof IOException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private RuntimeException propagate(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new DownstreamCallException("downstream call failed", failure);
    }
}

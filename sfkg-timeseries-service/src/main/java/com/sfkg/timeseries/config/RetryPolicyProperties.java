package com.sfkg.timeseries.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Shared downstream-call timeout and retry settings. */
@Component
@ConfigurationProperties(prefix = "timeseries.retry")
public class RetryPolicyProperties {

    private long responseTimeoutMillis = 5_000;
    private List<Long> backoffSeconds = new ArrayList<>(List.of(10L, 20L, 40L));

    public long getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    public void setResponseTimeoutMillis(long responseTimeoutMillis) {
        this.responseTimeoutMillis = responseTimeoutMillis;
    }

    public List<Long> getBackoffSeconds() {
        return backoffSeconds;
    }

    public void setBackoffSeconds(List<Long> backoffSeconds) {
        this.backoffSeconds = backoffSeconds == null ? new ArrayList<>() : new ArrayList<>(backoffSeconds);
    }
}

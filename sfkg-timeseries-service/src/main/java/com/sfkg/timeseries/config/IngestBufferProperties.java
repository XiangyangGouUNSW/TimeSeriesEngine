package com.sfkg.timeseries.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration for the ingest buffer-pool pipeline.
 */
@Component
@ConfigurationProperties(prefix = "timeseries.ingest")
public class IngestBufferProperties {

    /** Number of sender threads (hash-partition count). Default 4. */
    private int senderThreads = 4;

    /** Max points to accumulate before sending a batch. Default 200. */
    private int batchSize = 200;

    /** Max idle time (ms) before sending a partial batch. Default 500. */
    private long batchTimeoutMs = 500;

    /** Max queue capacity per sender thread. Default 10 000. */
    private int queueCapacity = 10_000;

    // ── getters / setters ──────────────────────────────────────────

    public int getSenderThreads() { return senderThreads; }
    public void setSenderThreads(int senderThreads) { this.senderThreads = senderThreads; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public long getBatchTimeoutMs() { return batchTimeoutMs; }
    public void setBatchTimeoutMs(long batchTimeoutMs) { this.batchTimeoutMs = batchTimeoutMs; }

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
}

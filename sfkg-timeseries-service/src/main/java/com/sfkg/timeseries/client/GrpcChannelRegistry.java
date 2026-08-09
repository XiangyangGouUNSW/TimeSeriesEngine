package com.sfkg.timeseries.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;

/**
 * Long-lived {@link ManagedChannel} registry — one channel per downstream
 * address, shared across all REST worker threads.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>Channel created lazily on first use via {@link #getChannel(String)}</li>
 *   <li>All calls share the same channel (thread-safe by design)</li>
 *   <li>On application shutdown: {@code shutdown → awaitTermination → shutdownNow}</li>
 * </ol>
 *
 * <h3>Observability</h3>
 * <ul>
 *   <li>{@link #getChannelCount()} — number of managed channels</li>
 *   <li>{@link #getChannelState(String)} — current connectivity state</li>
 *   <li>{@link #getMetrics()} — creation count and per-channel state snapshot</li>
 * </ul>
 */
@Component
public class GrpcChannelRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcChannelRegistry.class);
    private static final long SHUTDOWN_TIMEOUT_SEC = 5;

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final AtomicInteger creationCount = new AtomicInteger(0);

    /**
     * Returns a shared, long-lived channel for the given address.
     * Creates one if it doesn't already exist.
     */
    public ManagedChannel getChannel(String address) {
        return channels.computeIfAbsent(address, addr -> {
            ManagedChannel ch = ManagedChannelBuilder.forTarget(addr)
                    .usePlaintext()
                    .build();
            int count = creationCount.incrementAndGet();
            LOG.info("gRPC channel #{} created for {}", count, addr);
            return ch;
        });
    }

    /** Total number of channels ever created. */
    public int getCreationCount() {
        return creationCount.get();
    }

    /** Current number of managed channels. */
    public int getChannelCount() {
        return channels.size();
    }

    /** Returns the current {@link ConnectivityState} for the given address, or {@code null}. */
    public ConnectivityState getChannelState(String address) {
        ManagedChannel ch = channels.get(address);
        return ch != null ? ch.getState(false) : null;
    }

    /**
     * Snapshot of registry metrics suitable for actuator or logging.
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new java.util.LinkedHashMap<>();
        metrics.put("channelCount", channels.size());
        metrics.put("totalCreations", creationCount.get());
        Map<String, String> states = new java.util.LinkedHashMap<>();
        channels.forEach((addr, ch) -> states.put(addr, ch.getState(false).name()));
        metrics.put("states", states);
        return metrics;
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down {} gRPC channel(s)...", channels.size());
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            ManagedChannel ch = entry.getValue();
            String addr = entry.getKey();
            try {
                ch.shutdown();
                if (ch.awaitTermination(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    LOG.info("gRPC channel {} shutdown cleanly", addr);
                } else {
                    LOG.warn("gRPC channel {} did not terminate in {}s, forcing shutdownNow", addr, SHUTDOWN_TIMEOUT_SEC);
                    ch.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ch.shutdownNow();
                LOG.warn("gRPC channel {} shutdown interrupted, forced shutdownNow", addr);
            }
        }
        channels.clear();
        LOG.info("All gRPC channels shut down");
    }
}

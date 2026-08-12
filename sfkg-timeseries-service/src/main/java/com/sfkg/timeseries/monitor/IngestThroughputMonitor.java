package com.sfkg.timeseries.monitor;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Per-second ingest throughput monitor.
 * <p>
 * Writes one line per second to {@code logs/throughput.log} via the
 * dedicated {@code throughput} logger (see {@code logback-spring.xml}).
 * </p>
 *
 * <pre>
 *   [2026-08-12T10:30:01Z] recv=12500 recv/s=1250 sent=12480 sent/s=1248 err=0 err/s=0 queue=342
 * </pre>
 */
@Component
public class IngestThroughputMonitor {

    private static final Logger THROUGHPUT_LOG = LoggerFactory.getLogger("throughput");
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.of("UTC"));

    // ── cumulative counters (monotonically increasing) ──────────────
    private final AtomicLong receivedTotal = new AtomicLong();
    private final AtomicLong sentTotal = new AtomicLong();
    private final AtomicLong errorTotal = new AtomicLong();

    // ── last snapshot for delta / per-second rate ───────────────────
    private volatile long lastReceived;
    private volatile long lastSent;
    private volatile long lastError;
    private volatile long lastTimestamp;

    /** External callback: called by the HTTP ingest layer for every point enqueued. */
    public void recordReceived(long count) {
        receivedTotal.addAndGet(count);
    }

    /** External callback: called by the sender thread after a successful gRPC ingest. */
    public void recordSent(long count) {
        sentTotal.addAndGet(count);
    }

    /** External callback: called by the sender thread after a failed gRPC ingest. */
    public void recordError(long count) {
        errorTotal.addAndGet(count);
    }

    /** Snapshot of current queue backlog (provided by caller). */
    @Scheduled(fixedRateString = "${timeseries.monitor.interval-ms:1000}")
    public void report() {
        long now = System.currentTimeMillis();

        long curRecv = receivedTotal.get();
        long curSent = sentTotal.get();
        long curErr  = errorTotal.get();

        // ── first call: just capture baseline, don't log ────────────
        if (lastTimestamp == 0) {
            lastReceived  = curRecv;
            lastSent      = curSent;
            lastError     = curErr;
            lastTimestamp = now;
            return;
        }

        double elapsedSec = (now - lastTimestamp) / 1000.0;
        if (elapsedSec <= 0) {
            return;
        }

        double recvRate = (curRecv - lastReceived) / elapsedSec;
        double sentRate = (curSent - lastSent) / elapsedSec;
        double errRate  = (curErr - lastError) / elapsedSec;

        THROUGHPUT_LOG.info("{} recv={} recv/s={} sent={} sent/s={} err={} err/s={}",
                TS_FMT.format(Instant.now()),
                curRecv, String.format("%.1f", recvRate),
                curSent, String.format("%.1f", sentRate),
                curErr,  String.format("%.1f", errRate));

        // ── advance snapshot ────────────────────────────────────────
        lastReceived  = curRecv;
        lastSent      = curSent;
        lastError     = curErr;
        lastTimestamp = now;
    }

    // ── getters for diagnostics / ad-hoc queries ────────────────────

    public long getReceivedTotal() { return receivedTotal.get(); }
    public long getSentTotal()     { return sentTotal.get(); }
    public long getErrorTotal()    { return errorTotal.get(); }
}

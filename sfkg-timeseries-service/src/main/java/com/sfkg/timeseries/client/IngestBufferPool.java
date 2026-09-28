package com.sfkg.timeseries.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sfkg.timeseries.common.DownstreamUnavailableException;
import com.sfkg.timeseries.config.IngestBufferProperties;
import com.sfkg.timeseries.config.RetryPolicyProperties;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.monitor.IngestThroughputMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Hash-partitioned ingest buffer pool — one queue + one sender thread
 * per partition, guaranteeing that a given {@code seq_id} is always
 * delivered to Core by the same sender thread.
 *
 * <h3>Flow</h3>
 * <pre>
 * HTTP Thread            Sender Thread (N instances)
 *   │                        │
 *   ├─ hash(seqId)%N → idx   │
 *   ├─ queue[idx].offer()    │
 *   └─ return 200            │
 *                      ┌─ drain up to batchSize
 *                      ├─ build IngestDataRequest
 *                      ├─ gRPC ingestData
 *                      └─ on failure → in-memory retry with backoff
 * </pre>
 *
 * Failed batches go back to the <b>head</b> of their partition queue after a short backoff,
 * so the queues themselves are the retry buffer: nothing is written to disk, nothing is
 * dropped while the pool is running, and the backlog created by a Core outage is delivered
 * in order before newer data.
 */
@Component
public class IngestBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(IngestBufferPool.class);

    private final IngestBufferProperties props;
    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final IngestThroughputMonitor throughputMonitor;
    private final RetryPolicyProperties retryPolicyProperties;
    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "ingest-retry-scheduler");
        thread.setDaemon(true);
        return thread;
    });

    private BlockingDeque<TimeseriesDataSaveRequest.IngestPointDTO>[] queues;
    private Thread[] senders;
    private volatile boolean running = true;

    @SuppressWarnings("unchecked")
    public IngestBufferPool(IngestBufferProperties props, TimeseriesCoreGrpcClient coreGrpcClient,
                            IngestThroughputMonitor throughputMonitor,
                            RetryPolicyProperties retryPolicyProperties) {
        this.props = props;
        this.coreGrpcClient = coreGrpcClient;
        this.throughputMonitor = throughputMonitor;
        this.retryPolicyProperties = retryPolicyProperties;
    }

    @PostConstruct
    @SuppressWarnings("unchecked")
    void start() {
        int n = props.getSenderThreads();
        queues = new BlockingDeque[n];
        senders = new Thread[n];

        for (int i = 0; i < n; i++) {
            queues[i] = new LinkedBlockingDeque<>(props.getQueueCapacity());
            senders[i] = new Thread(new IngestSender(i, queues[i]), "ingest-sender-" + i);
            senders[i].setDaemon(true);
            senders[i].start();
        }

        LOG.info("Ingest buffer pool started — {} senders, batchSize={} timeout={}ms queueCapacity={}",
                n, props.getBatchSize(), props.getBatchTimeoutMs(), props.getQueueCapacity());
    }

    /**
     * Map a sequence ID to a partition index (0 .. senderThreads-1).
     */
    public int partition(String seqId) {
        return partition(null, seqId);
    }

    /** Delay before a failed batch is put back into its queue (walks the configured list). */
    private long backoffDelay(int index) {
        List<Long> backoff = retryPolicyProperties.getBackoffSeconds();
        if (backoff == null || backoff.isEmpty()) {
            return 0L;
        }
        int bounded = Math.min(Math.max(index, 0), backoff.size() - 1);
        return Math.max(0L, backoff.get(bounded));
    }

    public int partition(String projectId, String seqId) {
        if (seqId == null) {
            return 0;
        }
        int h = (String.valueOf(projectId) + "::" + seqId).hashCode();
        return Math.abs(h ^ (h >>> 16)) % props.getSenderThreads();
    }

    /**
     * Offer a point into the partition queue. Throws {@link DownstreamUnavailableException}
     * if the queue is full (back-pressure signal → HTTP 503).
     */
    public void offer(TimeseriesDataSaveRequest.IngestPointDTO point, int partition) {
        if (!queues[partition].offer(point)) {
            throw new DownstreamUnavailableException(
                    "ingest queue " + partition + " is full, try later", null);
        }
    }

    /** Total queued points across all partitions (best-effort snapshot). */
    public int getQueuedCount() {
        int total = 0;
        for (BlockingDeque<?> q : queues) {
            total += q.size();
        }
        return total;
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        for (Thread t : senders) {
            if (t != null) {
                t.interrupt();
            }
        }
        retryScheduler.shutdownNow();
        LOG.info("Ingest buffer pool shut down");
    }

    // ── Sender runnable ──────────────────────────────────────────────

    private class IngestSender implements Runnable {
        private final int index;
        private final BlockingDeque<TimeseriesDataSaveRequest.IngestPointDTO> queue;

        IngestSender(int index, BlockingDeque<TimeseriesDataSaveRequest.IngestPointDTO> queue) {
            this.index = index;
            this.queue = queue;
        }

        @Override
        public void run() {
            LOG.info("Ingest sender-{} started", index);
            while (running) {
                try {
                    List<TimeseriesDataSaveRequest.IngestPointDTO> batch = drain();
                    if (batch.isEmpty()) {
                        continue; // interrupted or shutdown
                    }
                    sendBatch(batch);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // drain remaining and exit
                    drainRemainingAndSend();
                    break;
                } catch (RuntimeException e) {
                    // A persistence or client-side error must not silently kill this sender.
                    LOG.error("Ingest sender-{} failed while processing a batch", index, e);
                }
            }
            LOG.info("Ingest sender-{} stopped", index);
        }

        /**
         * Drain up to {@code batchSize} points from the queue.
         * Blocks up to {@code batchTimeoutMs} for the first element,
         * then drains whatever is immediately available.
         */
        private List<TimeseriesDataSaveRequest.IngestPointDTO> drain() throws InterruptedException {
            List<TimeseriesDataSaveRequest.IngestPointDTO> batch = new ArrayList<>();

            // Wait for first element (with timeout)
            TimeseriesDataSaveRequest.IngestPointDTO first =
                    queue.poll(props.getBatchTimeoutMs(), TimeUnit.MILLISECONDS);
            if (first == null) {
                return batch; // timeout, empty
            }
            batch.add(first);

            // Drain remaining (non-blocking) up to batchSize
            int remaining = props.getBatchSize() - 1;
            for (int i = 0; i < remaining; i++) {
                TimeseriesDataSaveRequest.IngestPointDTO next = queue.poll();
                if (next == null) break;
                batch.add(next);
            }

            return batch;
        }

        private void sendBatch(List<TimeseriesDataSaveRequest.IngestPointDTO> batch) {
            Map<String, List<TimeseriesDataSaveRequest.IngestPointDTO>> byProject =
                    batch.stream().collect(java.util.stream.Collectors.groupingBy(
                            point -> String.valueOf(point.getProjectId())));
            for (Map.Entry<String, List<TimeseriesDataSaveRequest.IngestPointDTO>> entry : byProject.entrySet()) {
                deliver(entry.getValue());
            }
        }

        /**
         * Send one project's batch. A failed batch goes back into its partition queue
         * (after a backoff) instead of being retried inline or persisted to disk — the
         * queues are the retry buffer, so nothing is dropped while the pool is running.
         */
        private void deliver(List<TimeseriesDataSaveRequest.IngestPointDTO> points) {
            if (points == null || points.isEmpty()) {
                return;
            }
            TimeseriesDataSaveRequest request = new TimeseriesDataSaveRequest();
            request.setProjectId(points.get(0).getProjectId());
            request.setPoints(points);
            try {
                SyncResult result = coreGrpcClient.ingestData(request);
                if (result.isSuccess()) {
                    throughputMonitor.recordSent(points.size());
                    return;
                }
                requeue(points, result.getMessage());
            } catch (RuntimeException exception) {
                requeue(points, exception.getMessage());
            }
        }

        /** Put a failed batch back into its partition queue after the shared backoff. */
        private void requeue(List<TimeseriesDataSaveRequest.IngestPointDTO> points, String reason) {
            if (!running) {
                LOG.warn("Ingest sender-{} stopping; {} points not re-queued: {}", index, points.size(), reason);
                return;
            }
            long delaySeconds = backoffDelay(0);
            LOG.warn("Ingest sender-{} requeueing {} points in {}s after failure: {}",
                    index, points.size(), delaySeconds, reason);
            throughputMonitor.recordError(points.size());
            retryScheduler.schedule(() -> requeueInto(points), delaySeconds, TimeUnit.SECONDS);
        }

        /**
         * Push deferred points back to the <b>head</b> of the partition that owns their
         * sequence ID, so the failed batch is retried before newer data.
         */
        private void requeueInto(List<TimeseriesDataSaveRequest.IngestPointDTO> points) {
            if (!running || points == null || points.isEmpty()) {
                return;
            }
            Map<Integer, List<TimeseriesDataSaveRequest.IngestPointDTO>> byPartition = new HashMap<>();
            for (TimeseriesDataSaveRequest.IngestPointDTO point : points) {
                int partition = partition(point.getProjectId(), point.getSequenceId());
                byPartition.computeIfAbsent(partition, key -> new ArrayList<>()).add(point);
            }
            List<TimeseriesDataSaveRequest.IngestPointDTO> deferred = new ArrayList<>();
            for (Map.Entry<Integer, List<TimeseriesDataSaveRequest.IngestPointDTO>> entry : byPartition.entrySet()) {
                List<TimeseriesDataSaveRequest.IngestPointDTO> group = entry.getValue();
                // offerFirst in reverse so the batch keeps its original order at the head
                for (int i = group.size() - 1; i >= 0; i--) {
                    if (!queues[entry.getKey()].offerFirst(group.get(i))) {
                        deferred.add(group.get(i));
                    }
                }
            }
            if (!deferred.isEmpty()) {
                long delaySeconds = backoffDelay(1);
                LOG.warn("Ingest requeue: {} points did not fit into full queues, retrying in {}s",
                        deferred.size(), delaySeconds);
                retryScheduler.schedule(() -> requeueInto(deferred), delaySeconds, TimeUnit.SECONDS);
            }
        }

        /** Drain everything left in the queue and send one last batch. */
        private void drainRemainingAndSend() {
            List<TimeseriesDataSaveRequest.IngestPointDTO> batch = new ArrayList<>();
            queue.drainTo(batch);
            if (!batch.isEmpty()) {
                sendBatch(batch);
            }
        }
    }
}

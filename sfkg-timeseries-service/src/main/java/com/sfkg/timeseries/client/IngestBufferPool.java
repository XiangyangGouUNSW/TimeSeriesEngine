package com.sfkg.timeseries.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.config.IngestBufferProperties;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;

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
 *                      └─ on response → flush, loop
 * </pre>
 */
@Component
public class IngestBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(IngestBufferPool.class);

    private final IngestBufferProperties props;
    private final TimeseriesCoreGrpcClient coreGrpcClient;

    private BlockingQueue<TimeseriesDataSaveRequest.IngestPointDTO>[] queues;
    private Thread[] senders;
    private volatile boolean running = true;

    @SuppressWarnings("unchecked")
    public IngestBufferPool(IngestBufferProperties props, TimeseriesCoreGrpcClient coreGrpcClient) {
        this.props = props;
        this.coreGrpcClient = coreGrpcClient;
    }

    @PostConstruct
    @SuppressWarnings("unchecked")
    void start() {
        int n = props.getSenderThreads();
        queues = new BlockingQueue[n];
        senders = new Thread[n];

        for (int i = 0; i < n; i++) {
            queues[i] = new LinkedBlockingQueue<>(props.getQueueCapacity());
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
        if (seqId == null) {
            return 0;
        }
        return Math.abs(seqId.hashCode()) % props.getSenderThreads();
    }

    /**
     * Offer a point into the partition queue. Throws {@link BusinessException}
     * if the queue is full (back-pressure signal → HTTP 503).
     */
    public void offer(TimeseriesDataSaveRequest.IngestPointDTO point, int partition) {
        if (!queues[partition].offer(point)) {
            throw new BusinessException("ingest queue " + partition + " is full, try later");
        }
    }

    /** Total queued points across all partitions (best-effort snapshot). */
    public int getQueuedCount() {
        int total = 0;
        for (BlockingQueue<?> q : queues) {
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
        LOG.info("Ingest buffer pool shut down");
    }

    // ── Sender runnable ──────────────────────────────────────────────

    private class IngestSender implements Runnable {
        private final int index;
        private final BlockingQueue<TimeseriesDataSaveRequest.IngestPointDTO> queue;

        IngestSender(int index, BlockingQueue<TimeseriesDataSaveRequest.IngestPointDTO> queue) {
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
            TimeseriesDataSaveRequest request = new TimeseriesDataSaveRequest();
            request.setPoints(batch);

            LOG.debug("Ingest sender-{} sending batch size={}", index, batch.size());
            SyncResult result = coreGrpcClient.ingestData(request);
            if (!result.isSuccess()) {
                LOG.warn("Ingest sender-{} batch failed ({} points): {}",
                        index, batch.size(), result.getMessage());
            } else {
                LOG.debug("Ingest sender-{} batch ok ({} points)", index, batch.size());
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

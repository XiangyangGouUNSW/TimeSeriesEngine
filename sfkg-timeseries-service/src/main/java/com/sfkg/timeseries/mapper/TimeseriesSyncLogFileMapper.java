package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesSyncLog;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesSyncLogFileMapper implements TimeseriesSyncLogMapper {

    private final LocalJsonTableStore<TimeseriesSyncLog> store;

    public TimeseriesSyncLogFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-sync-log.json",
                TimeseriesSyncLog.class);
    }

    @Override
    public void insertSyncLog(TimeseriesSyncLog log) {
        if (log != null && log.getId() == null) {
            log.setId(nextId());
        }
        store.upsert(item -> sameBusinessKey(log, item), log);
    }

    @Override
    public void updateSyncResult(TimeseriesSyncLog log) {
        insertSyncLog(log);
    }

    @Override
    public List<TimeseriesSyncLog> selectAll() {
        return store.readAll();
    }

    @Override
    public List<TimeseriesSyncLog> selectFailedLogs() {
        return store.readAll().stream()
                .filter(log -> "FAILED".equalsIgnoreCase(log.getSyncStatus()))
                .collect(Collectors.toList());
    }

    @Override
    public void markRetried(String id) {
        store.update(
                log -> Objects.equals(id, log.getId()),
                log -> {
                    Integer retryTimes = log.getRetryTimes() == null ? 0 : log.getRetryTimes();
                    log.setRetryTimes(retryTimes + 1);
                    log.setLastRetryTime(LocalDateTime.now());
                });
    }

    private String nextId() {
        return UUID.randomUUID().toString();
    }

    private boolean sameBusinessKey(TimeseriesSyncLog incoming, TimeseriesSyncLog stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getId(), stored.getId());
    }
}

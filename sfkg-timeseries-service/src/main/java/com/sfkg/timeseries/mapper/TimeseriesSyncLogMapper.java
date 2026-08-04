package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesSyncLog;
import java.util.List;

public interface TimeseriesSyncLogMapper {

    void insertSyncLog(TimeseriesSyncLog log);

    void updateSyncResult(TimeseriesSyncLog log);

    List<TimeseriesSyncLog> selectAll();

    List<TimeseriesSyncLog> selectFailedLogs();

    void markRetried(Integer id);
}

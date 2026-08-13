package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.StatisticsQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesStatisticsResult;
import java.util.List;

public interface TimeseriesStatisticsService {

    TimeseriesStatisticsResult computeAndStore(StatisticsQueryRequest request);

    List<TimeseriesStatisticsResult> listResults();
}

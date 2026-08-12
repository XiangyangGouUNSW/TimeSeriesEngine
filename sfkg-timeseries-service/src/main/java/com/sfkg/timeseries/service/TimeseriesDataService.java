package com.sfkg.timeseries.service;

import java.util.Map;

import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.vo.HistoryDataVO;

public interface TimeseriesDataService {

    String saveTimeseriesData(TimeseriesDataSaveRequest request);

    HistoryDataVO queryHistoryData(HistoryDataQueryRequest request);

    Map<String, Object> queryHistoryOverview(HistoryDataQueryRequest request);

    Map<String, Object> queryWindowData(HistoryDataQueryRequest request);

    void validateHistoryQuery(HistoryDataQueryRequest request);
}

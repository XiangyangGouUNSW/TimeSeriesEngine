package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.vo.HistoryDataVO;
import java.util.Map;

public interface TimeseriesDataService {

    String saveTimeseriesData(TimeseriesDataSaveRequest request);

    HistoryDataVO queryHistoryData(HistoryDataQueryRequest request);

    Map<String, Object> queryHistoryOverview(HistoryDataQueryRequest request);

    Map<String, Object> queryWindowData(HistoryDataQueryRequest request);

    void validateHistoryQuery(HistoryDataQueryRequest request);

    HistoryDataVO queryHistoryFromTdengine(HistoryDataQueryRequest request);

    HistoryDataVO queryHistoryFromCoreGrpc(HistoryDataQueryRequest request);

    HistoryDataVO convertHistoryData(Object rawData);
}

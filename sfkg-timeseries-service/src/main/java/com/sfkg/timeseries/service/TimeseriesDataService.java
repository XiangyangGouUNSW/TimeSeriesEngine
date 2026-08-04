package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.vo.HistoryDataVO;

public interface TimeseriesDataService {

    Integer saveTimeseriesData(TimeseriesDataSaveRequest request);

    HistoryDataVO queryHistoryData(HistoryDataQueryRequest request);

    void validateHistoryQuery(HistoryDataQueryRequest request);

    HistoryDataVO queryHistoryFromTdengine(HistoryDataQueryRequest request);

    HistoryDataVO queryHistoryFromCoreGrpc(HistoryDataQueryRequest request);

    HistoryDataVO convertHistoryData(Object rawData);
}

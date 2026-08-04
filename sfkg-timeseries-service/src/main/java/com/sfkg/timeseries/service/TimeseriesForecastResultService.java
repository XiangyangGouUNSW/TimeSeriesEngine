package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.ForecastResultQueryRequest;
import com.sfkg.timeseries.vo.ForecastResultVO;

public interface TimeseriesForecastResultService {

    ForecastResultVO queryForecastResults(ForecastResultQueryRequest request);

    ForecastResultVO handleForecastResult(Object rawResult);

    Integer createWarningEvent(ForecastResultVO result);
}

package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.vo.AnomalyResultVO;

public interface TimeseriesAnomalyResultService {

    AnomalyResultVO queryAnomalyResults(AnomalyResultQueryRequest request);

    AnomalyResultVO handleAnomalyResult(Object rawResult);

    String createAnomalyEvent(AnomalyResultVO result);
}

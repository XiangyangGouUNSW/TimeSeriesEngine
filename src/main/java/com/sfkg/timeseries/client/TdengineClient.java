package com.sfkg.timeseries.client;

import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.vo.HistoryDataVO;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;

@Component
public class TdengineClient {

    public HistoryDataVO queryHistoryData(HistoryDataQueryRequest request) {
        return new HistoryDataVO();
    }

    public LocalDateTime queryLatestDataTime(Integer sequenceId) {
        return null;
    }

    public HistoryDataVO aggregateHistoryData(HistoryDataQueryRequest request) {
        return new HistoryDataVO();
    }
}

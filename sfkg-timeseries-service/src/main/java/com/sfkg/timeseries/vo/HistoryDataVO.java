package com.sfkg.timeseries.vo;

import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import java.util.List;
import lombok.Data;

@Data
public class HistoryDataVO {

    private String sequenceId;
    private List<TimeseriesDataPoint> points;
}

package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class DiagnosisResultVO {

    private Integer eventId;
    private String diagnosisResult;
    private String diagnosisBasis;
}

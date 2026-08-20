package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class DiagnosisResultVO {

    private String projectId;

    private String eventId;
    private String diagnosisResult;
    private String diagnosisBasis;
}

package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class DisposalFeedbackRequest {
    private String projectId;

    private String eventId;
    private String disposalResult;
    private String handleStatus;
}

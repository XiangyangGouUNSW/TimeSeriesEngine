package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class DisposalFeedbackRequest {

    private Integer eventId;
    private String disposalResult;
    private String handleStatus;
}

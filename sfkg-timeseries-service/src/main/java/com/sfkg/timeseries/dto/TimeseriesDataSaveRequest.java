package com.sfkg.timeseries.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesDataSaveRequest {

    private String projectId;

    @JsonAlias("return_resolved_data")
    private Boolean returnResolvedData;

    private List<IngestPointDTO> points;

    @Data
    public static class IngestPointDTO {
        private String projectId;

        @JsonAlias("sequence_id")
        private String sequenceId;

        @JsonAlias("data_source_id")
        private String dataSourceId;

        @JsonAlias("external_sequence_id")
        private String externalSequenceId;

        private Long time;

        @JsonAlias("double_value")
        private Double doubleValue;

        @JsonAlias("int64_value")
        private Long int64Value;

        @JsonAlias("bool_value")
        private Boolean boolValue;

        @JsonAlias("string_value")
        private String stringValue;
    }
}

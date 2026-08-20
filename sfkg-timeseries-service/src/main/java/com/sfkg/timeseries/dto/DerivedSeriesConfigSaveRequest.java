package com.sfkg.timeseries.dto;

import java.util.List;
import lombok.Data;

@Data
public class DerivedSeriesConfigSaveRequest {
    private String projectId;

    private List<DerivedSeriesConfigItem> items;

    @Data
    public static class DerivedSeriesConfigItem {
        private String derivedSequenceId;
        private boolean enabled;
        private LinearCombinationDTO linearCombination;
        private DerivedExpressionDTO expression;
    }

    @Data
    public static class LinearTermDTO {
        private String sequenceId;
        private Double coefficient;
    }

    @Data
    public static class LinearCombinationDTO {
        private List<LinearTermDTO> terms;
        private Double bias;
    }

    @Data
    public static class DerivedExpressionDTO {
        private String sequenceId;
        private Double constant;
        private DerivedBinaryExpressionDTO binary;
    }

    @Data
    public static class DerivedBinaryExpressionDTO {
        private String operator;
        private DerivedExpressionDTO left;
        private DerivedExpressionDTO right;
    }
}

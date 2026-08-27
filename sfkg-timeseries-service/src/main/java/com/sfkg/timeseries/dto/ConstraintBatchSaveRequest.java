package com.sfkg.timeseries.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Data;

/**
 * OR 组批量创建请求：一次提交多条约束，共享同一个 orGroupId，
 * 后端整体校验、整体落库后一次性同步到 Core，保证 OR 组原子送达。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConstraintBatchSaveRequest {

    private String projectId;

    /** OR 组 ID（必填）：组内任一满足即组满足，组间 AND。 */
    private String orGroupId;

    /** 组成员约束列表（至少 1 条）。 */
    private List<ConstraintSaveRequest> constraints;
}

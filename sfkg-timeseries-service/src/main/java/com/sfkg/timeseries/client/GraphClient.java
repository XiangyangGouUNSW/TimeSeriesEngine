package com.sfkg.timeseries.client;

import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class GraphClient {

    public boolean checkDeviceInstanceExists(Integer deviceInstanceId) {
        return true;
    }

    public SyncResult upsertTimeseriesInstanceNode(TimeseriesInstanceConfig config) {
        return SyncResult.success();
    }

    public SyncResult upsertCategoryNode(TimeseriesCategory category) {
        return SyncResult.success();
    }

    public SyncResult upsertConstraintNode(TimeseriesConstraint constraint) {
        return SyncResult.success();
    }

    public SyncResult upsertRelation(TimeseriesRelation relation) {
        return SyncResult.success();
    }

    public SyncResult upsertEventNode(TimeseriesEvent event) {
        return SyncResult.success();
    }

    public SyncResult linkInstanceCategory(Integer sequenceId, Integer categoryId) {
        return SyncResult.success();
    }

    public SyncResult linkInstanceDevice(Integer sequenceId, Integer deviceInstanceId) {
        return SyncResult.success();
    }

    public SyncResult linkEventRelations(Integer eventId) {
        return SyncResult.success();
    }

    public Map<String, Object> queryEventSemanticContext(Integer eventId) {
        return Map.of();
    }

    public SyncResult updateEventFeedback(Integer eventId, String disposalResult, String handleStatus) {
        return SyncResult.success();
    }
}

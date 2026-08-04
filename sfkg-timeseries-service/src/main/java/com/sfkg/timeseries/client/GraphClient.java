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

    public boolean checkDeviceInstanceExists(String deviceInstanceId) {
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

    public SyncResult linkInstanceCategory(String sequenceId, String categoryId) {
        return SyncResult.success();
    }

    public SyncResult linkInstanceDevice(String sequenceId, String deviceInstanceId) {
        return SyncResult.success();
    }

    public SyncResult linkEventRelations(String eventId) {
        return SyncResult.success();
    }

    public Map<String, Object> queryEventSemanticContext(String eventId) {
        return Map.of();
    }

    public SyncResult updateEventFeedback(String eventId, String disposalResult, String handleStatus) {
        return SyncResult.success();
    }
}

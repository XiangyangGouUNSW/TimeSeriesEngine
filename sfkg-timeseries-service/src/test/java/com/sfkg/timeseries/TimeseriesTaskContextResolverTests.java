package com.sfkg.timeseries;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.grpc.SemanticContext;
import com.sfkg.timeseries.service.TimeseriesConstraintExpansionResolver;
import com.sfkg.timeseries.service.TimeseriesTaskContextResolver;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TimeseriesTaskContextResolverTests {

    @Test
    void forecastContextUsesExpandedConstraintIds() {
        TimeseriesMemoryCache cache = new TimeseriesMemoryCache();
        cache.putCategory(category("OT"));
        cache.putInstanceConfig(instance("ETTh1_OT", "OT", "device-1"));
        cache.putInstanceConfig(instance("ETTh2_OT", "OT", "device-2"));
        cache.putConstraint(categoryConstraint());

        TimeseriesConstraintExpansionResolver expansionResolver =
                new TimeseriesConstraintExpansionResolver(cache);
        TimeseriesTaskContextResolver contextResolver =
                new TimeseriesTaskContextResolver(cache, expansionResolver);

        TimeseriesForecastTask task = new TimeseriesForecastTask();
        task.setForecastObjects(List.of("ETTh1_OT"));

        SemanticContext context = contextResolver.resolveForecastContext(task);

        assertTrue(context.getConstraintIdsList().contains("c-temp-range_ETTh1_OT"));
        assertFalse(context.getConstraintIdsList().contains("c-temp-range"));
        assertFalse(context.getConstraintIdsList().contains("c-temp-range_ETTh2_OT"));
    }

    private TimeseriesCategory category(String categoryId) {
        TimeseriesCategory category = new TimeseriesCategory();
        category.setCategoryId(categoryId);
        category.setCategoryName(categoryId);
        category.setConfirmStatus("CONFIRMED");
        return category;
    }

    private TimeseriesInstanceConfig instance(String sequenceId, String categoryId, String deviceInstanceId) {
        TimeseriesInstanceConfig instance = new TimeseriesInstanceConfig();
        instance.setSequenceId(sequenceId);
        instance.setInstanceName(sequenceId);
        instance.setCategoryId(categoryId);
        instance.setDeviceInstanceId(deviceInstanceId);
        instance.setDataType("NUMERIC");
        return instance;
    }

    private TimeseriesConstraint categoryConstraint() {
        TimeseriesConstraint constraint = new TimeseriesConstraint();
        constraint.setConstraintId("c-temp-range");
        constraint.setConstraintName("temperature range");
        constraint.setVariableMapping(Map.of("x", "OT"));
        constraint.setConstraintExpression("x <= 100");
        constraint.setLowerBound(0.0);
        constraint.setUpperBound(100.0);
        constraint.setEffectiveStatus("ENABLE");
        constraint.setConfirmStatus("CONFIRMED");
        return constraint;
    }
}

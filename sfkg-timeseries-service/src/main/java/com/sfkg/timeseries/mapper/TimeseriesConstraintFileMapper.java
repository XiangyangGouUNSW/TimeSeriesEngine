package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.ConstraintQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesConstraintFileMapper implements TimeseriesConstraintMapper {

    private final LocalJsonTableStore<TimeseriesConstraint> store;

    public TimeseriesConstraintFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-constraint.json",
                TimeseriesConstraint.class);
    }

    @Override
    public void insert(TimeseriesConstraint entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
    }

    @Override
    public void updateById(TimeseriesConstraint entity) {
        insert(entity);
    }

    @Override
    public TimeseriesConstraint selectById(String constraintId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(constraintId, entity.getConstraintId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesConstraint> selectByCondition(Object condition) {
        return store.readAll().stream()
                .filter(entity -> matches(condition, entity))
                .collect(Collectors.toList());
    }

    @Override
    public void updateStatus(String constraintId, String status) {
        store.update(
                entity -> Objects.equals(constraintId, entity.getConstraintId()),
                entity -> entity.setEffectiveStatus(status));
    }

    private boolean sameBusinessKey(TimeseriesConstraint incoming, TimeseriesConstraint stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getConstraintId(), stored.getConstraintId());
    }

    private boolean matches(Object condition, TimeseriesConstraint entity) {
        if (condition == null) {
            return true;
        }
        if (!(condition instanceof ConstraintQueryRequest request)) {
            return true;
        }
        return equalsIfPresent(request.getConstraintId(), entity.getConstraintId())
                && containsIfPresent(request.getConstraintName(), entity.getConstraintName())
                && equalsTextIfPresent(request.getEffectiveStatus(), entity.getEffectiveStatus())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && matchesKeyword(request.getKeyword(), entity);
    }

    private boolean equalsIfPresent(String expected, String actual) {
        return expected == null || Objects.equals(expected, actual);
    }

    private boolean equalsTextIfPresent(String expected, String actual) {
        return expected == null || (actual != null && expected.equalsIgnoreCase(actual));
    }

    private boolean containsIfPresent(String keyword, String actual) {
        return keyword == null
                || (actual != null && actual.toLowerCase().contains(keyword.toLowerCase()));
    }

    private boolean matchesKeyword(String keyword, TimeseriesConstraint entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getConstraintName())
                || containsIfPresent(keyword, entity.getConstraintDescription())
                || containsIfPresent(keyword, entity.getConstraintExpression());
    }
}

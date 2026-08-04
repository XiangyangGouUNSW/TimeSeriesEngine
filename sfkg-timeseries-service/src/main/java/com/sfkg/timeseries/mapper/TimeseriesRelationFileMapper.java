package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.RelationQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesRelationFileMapper implements TimeseriesRelationMapper {

    private final LocalJsonTableStore<TimeseriesRelation> store;

    public TimeseriesRelationFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-relation.json",
                TimeseriesRelation.class);
    }

    @Override
    public void insert(TimeseriesRelation entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
    }

    @Override
    public void updateById(TimeseriesRelation entity) {
        insert(entity);
    }

    @Override
    public TimeseriesRelation selectById(String relationId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(relationId, entity.getRelationId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesRelation> selectByCondition(Object condition) {
        return store.readAll().stream()
                .filter(entity -> matches(condition, entity))
                .collect(Collectors.toList());
    }

    @Override
    public void updateStatus(String relationId, String status) {
        store.update(
                entity -> Objects.equals(relationId, entity.getRelationId()),
                entity -> entity.setEffectiveStatus(status));
    }

    private boolean sameBusinessKey(TimeseriesRelation incoming, TimeseriesRelation stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getRelationId(), stored.getRelationId());
    }

    private boolean matches(Object condition, TimeseriesRelation entity) {
        if (condition == null) {
            return true;
        }
        if (!(condition instanceof RelationQueryRequest request)) {
            return true;
        }
        return equalsIfPresent(request.getRelationId(), entity.getRelationId())
                && containsIfPresent(request.getRelationName(), entity.getRelationName())
                && sourceContains(request.getSourceCategoryId(), entity)
                && equalsIfPresent(request.getTargetCategoryId(), entity.getTargetCategoryId())
                && equalsTextIfPresent(request.getRelationType(), entity.getRelationType())
                && equalsTextIfPresent(request.getEffectiveStatus(), entity.getEffectiveStatus())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && matchesKeyword(request.getKeyword(), entity);
    }

    private boolean sourceContains(String sourceCategoryId, TimeseriesRelation entity) {
        return sourceCategoryId == null
                || (entity.getSourceCategories() != null && entity.getSourceCategories().contains(sourceCategoryId));
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

    private boolean matchesKeyword(String keyword, TimeseriesRelation entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getRelationName())
                || containsIfPresent(keyword, entity.getTargetCategoryName())
                || containsIfPresent(keyword, entity.getRelationType())
                || containsIfPresent(keyword, entity.getLagRange());
    }
}

package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.CategoryQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesCategoryFileMapper implements TimeseriesCategoryMapper {

    private final LocalJsonTableStore<TimeseriesCategory> store;

    public TimeseriesCategoryFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-category.json",
                TimeseriesCategory.class);
    }

    @Override
    public void insert(TimeseriesCategory entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
    }

    @Override
    public void updateById(TimeseriesCategory entity) {
        insert(entity);
    }

    @Override
    public TimeseriesCategory selectById(String categoryId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(categoryId, entity.getCategoryId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesCategory> selectByCondition(Object condition) {
        return store.readAll().stream()
                .filter(entity -> matches(condition, entity))
                .collect(Collectors.toList());
    }

    @Override
    public boolean existsConfirmedCategory(String categoryId) {
        TimeseriesCategory category = selectById(categoryId);
        return category != null && "CONFIRMED".equalsIgnoreCase(category.getConfirmStatus());
    }

    private boolean sameBusinessKey(TimeseriesCategory incoming, TimeseriesCategory stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getCategoryId(), stored.getCategoryId());
    }

    private boolean matches(Object condition, TimeseriesCategory entity) {
        if (condition == null) {
            return true;
        }
        if (!(condition instanceof CategoryQueryRequest request)) {
            return true;
        }
        return equalsIfPresent(request.getCategoryId(), entity.getCategoryId())
                && containsIfPresent(request.getCategoryName(), entity.getCategoryName())
                && equalsTextIfPresent(request.getDataType(), entity.getDataType())
                && equalsTextIfPresent(request.getApplicableObjectType(), entity.getApplicableObjectType())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus());
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
}

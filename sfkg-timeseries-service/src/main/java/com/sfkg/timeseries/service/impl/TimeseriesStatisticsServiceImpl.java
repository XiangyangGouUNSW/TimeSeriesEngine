package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.StatisticsQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.entity.TimeseriesStatisticsResult;
import com.sfkg.timeseries.grpc.AlignedWindowData;
import com.sfkg.timeseries.grpc.ComputeStatisticsResponse;
import com.sfkg.timeseries.grpc.CorrelationVector;
import com.sfkg.timeseries.grpc.NamedMetric;
import com.sfkg.timeseries.grpc.SequenceCorrelation;
import com.sfkg.timeseries.grpc.SequenceMetric;
import com.sfkg.timeseries.grpc.TimeseriesValue;
import com.sfkg.timeseries.mapper.TimeseriesStatisticsResultMapper;
import com.sfkg.timeseries.service.TimeseriesStatisticsService;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesStatisticsServiceImpl implements TimeseriesStatisticsService {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesStatisticsServiceImpl.class);

    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesStatisticsResultMapper resultMapper;

    public TimeseriesStatisticsServiceImpl(
            TimeseriesCoreGrpcClient coreGrpcClient,
            TimeseriesCacheManager cacheManager,
            TimeseriesMemoryCache memoryCache,
            TimeseriesStatisticsResultMapper resultMapper) {
        this.coreGrpcClient = coreGrpcClient;
        this.cacheManager = cacheManager;
        this.memoryCache = memoryCache;
        this.resultMapper = resultMapper;
    }

    @Override
    public TimeseriesStatisticsResult computeAndStore(StatisticsQueryRequest request) {
        List<String> sequenceIds = request != null && request.getSequenceIds() != null
                ? request.getSequenceIds()
                : List.of();
        if (sequenceIds.isEmpty()) {
            throw new BusinessException("statistics: sequenceIds must not be empty");
        }

        List<String> relationIds = resolveRelationIds(request, sequenceIds);

        ComputeStatisticsResponse response = alignAndCompute(
                sequenceIds, request.getDependentSequenceId(), relationIds,
                request.getStartTime(), request.getEndTime());
        if (response == null) {
            throw new BusinessException("statistics: computeBasicStatistics failed");
        }

        TimeseriesStatisticsResult result = toEntity(request, response);
        resultMapper.insert(result);
        LOG.info("[statistics] stored result {} seqs={} metrics={} correlations={}",
                result.getResultId(), result.getSequenceIds().size(),
                result.getSequenceMetrics().size(), result.getCorrelationCoefficients().size());
        return result;
    }

    @Override
    public List<TimeseriesStatisticsResult> listResults() {
        return resultMapper.selectAll();
    }

    private List<String> resolveRelationIds(StatisticsQueryRequest request, List<String> sequenceIds) {
        if (request.getRelationIds() == null || request.getRelationIds().isEmpty()) {
            return List.of();
        }
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        List<String> expanded = new ArrayList<>();
        for (String relationId : request.getRelationIds()) {
            if (relationId == null || relationId.isBlank()) {
                continue;
            }
            TimeseriesRelation relation = memoryCache.getRelation(relationId).orElse(null);
            if (relation == null) {
                throw new BusinessException("statistics: relation not found: " + relationId);
            }
            expanded.addAll(coreGrpcClient.resolveExpandedRelationIds(
                    relation, sequenceIds, request.getDependentSequenceId()));
        }
        return new ArrayList<>(new java.util.LinkedHashSet<>(expanded));
    }

    /**
     * Align the window and compute statistics. Core requires a unique target
     * sequence per {@code alignWindowData} call, so when multiple relations
     * share the same dependent (target) we align and compute one relation at a
     * time and merge the resulting correlation vectors.
     */
    private ComputeStatisticsResponse alignAndCompute(
            List<String> sequenceIds, String dependentSequenceId, List<String> relationIds,
            LocalDateTime startTime, LocalDateTime endTime) {
        if (relationIds == null || relationIds.isEmpty()) {
            // Metrics only; Core computes no correlation vector without a relation.
            AlignedWindowData aligned = coreGrpcClient.alignWindowData(
                    sequenceIds, dependentSequenceId, List.of(), startTime, endTime);
            if (aligned == null || aligned.getSamplesCount() == 0) {
                throw new BusinessException("statistics: alignWindowData returned empty data");
            }
            return coreGrpcClient.computeBasicStatistics(
                    aligned, sequenceIds, dependentSequenceId, null);
        }

        ComputeStatisticsResponse merged = null;
        for (String relationId : relationIds) {
            AlignedWindowData aligned = coreGrpcClient.alignWindowData(
                    sequenceIds, dependentSequenceId, List.of(relationId), startTime, endTime);
            if (aligned == null || aligned.getSamplesCount() == 0) {
                LOG.warn("[statistics] alignWindowData returned empty data for relation={}, skip", relationId);
                continue;
            }
            ComputeStatisticsResponse response = coreGrpcClient.computeBasicStatistics(
                    aligned, sequenceIds, dependentSequenceId, relationId);
            if (response == null) {
                LOG.warn("[statistics] computeBasicStatistics failed for relation={}, skip", relationId);
                continue;
            }
            if (merged == null) {
                merged = response;
                continue;
            }
            if (response.hasCorrelationVector()) {
                CorrelationVector.Builder vector = merged.hasCorrelationVector()
                        ? merged.getCorrelationVector().toBuilder()
                        : CorrelationVector.newBuilder();
                vector.addAllCorrelations(response.getCorrelationVector().getCorrelationsList());
                merged = merged.toBuilder().setCorrelationVector(vector.build()).build();
            }
        }
        return merged;
    }

    private TimeseriesStatisticsResult toEntity(
            StatisticsQueryRequest request, ComputeStatisticsResponse response) {
        TimeseriesStatisticsResult result = new TimeseriesStatisticsResult();
        result.setResultId(UUID.randomUUID().toString());
        result.setSequenceIds(new ArrayList<>(request.getSequenceIds()));
        result.setRelationIds(request.getRelationIds() != null
                ? new ArrayList<>(request.getRelationIds()) : List.of());
        result.setStartTime(request.getStartTime());
        result.setEndTime(request.getEndTime());
        result.setComputedAt(LocalDateTime.now());

        Map<String, Map<String, Double>> sequenceMetrics = new LinkedHashMap<>();
        for (SequenceMetric sequenceMetric : response.getSequenceMetricsList()) {
            Map<String, Double> metrics = new LinkedHashMap<>();
            for (NamedMetric namedMetric : sequenceMetric.getMetricsList()) {
                metrics.put(namedMetric.getName(), toDouble(namedMetric.getValue()));
            }
            sequenceMetrics.put(sequenceMetric.getSequenceId(), metrics);
        }
        result.setSequenceMetrics(sequenceMetrics);

        if (response.hasCorrelationVector()) {
            CorrelationVector vector = response.getCorrelationVector();
            result.setDependentSequenceId(vector.getDependentSequenceId());
            Map<String, Double> correlations = new LinkedHashMap<>();
            for (SequenceCorrelation correlation : vector.getCorrelationsList()) {
                correlations.put(correlation.getIndependentSequenceId(), correlation.getCoefficient());
            }
            result.setCorrelationCoefficients(correlations);
        } else {
            result.setDependentSequenceId(request.getDependentSequenceId());
            result.setCorrelationCoefficients(Map.of());
        }
        return result;
    }

    private Double toDouble(TimeseriesValue value) {
        if (value == null) {
            return null;
        }
        switch (value.getKindCase()) {
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            case INT64_VALUE:
                return (double) value.getInt64Value();
            case BOOL_VALUE:
                return value.getBoolValue() ? 1.0 : 0.0;
            case STRING_VALUE:
                try {
                    return Double.valueOf(value.getStringValue());
                } catch (NumberFormatException ignored) {
                    return null;
                }
            default:
                return null;
        }
    }
}

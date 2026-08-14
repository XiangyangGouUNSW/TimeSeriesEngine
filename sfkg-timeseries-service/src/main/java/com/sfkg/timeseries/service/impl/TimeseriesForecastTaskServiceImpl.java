package com.sfkg.timeseries.service.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.ForecastTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.mapper.TimeseriesForecastTaskMapper;
import com.sfkg.timeseries.service.TimeseriesForecastTaskService;
import com.sfkg.timeseries.vo.ForecastTaskVO;

@Service
public class TimeseriesForecastTaskServiceImpl implements TimeseriesForecastTaskService {

    private final TimeseriesForecastTaskMapper forecastTaskMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final ForecastGrpcClient forecastGrpcClient;

    public TimeseriesForecastTaskServiceImpl(
            TimeseriesForecastTaskMapper forecastTaskMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            ForecastGrpcClient forecastGrpcClient) {
        this.forecastTaskMapper = forecastTaskMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.forecastGrpcClient = forecastGrpcClient;
    }

    @Override
    public String createForecastTask(ForecastTaskSaveRequest request) {
        String taskId = request != null ? request.getTaskId() : null;
        if (taskId != null) {
            cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
            if (memoryCache.getForecastTask(taskId).isPresent()) {
                throw new BusinessException("forecast task already exists: " + taskId);
            }
        }
        return doSaveForecastTask(request);
    }

    @Override
    public String saveForecastTask(ForecastTaskSaveRequest request) {
        return doSaveForecastTask(request);
    }

    private String doSaveForecastTask(ForecastTaskSaveRequest request) {
        if (request == null) {
            throw new BusinessException("forecast task request must not be null");
        }
        validateForecastObjects(request);
        validateForecastHorizon(request.getForecastHorizon());
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        String taskId = request.getTaskId() == null
                ? generateTaskId()
                : request.getTaskId();

        TimeseriesForecastTask entity = memoryCache.computeForecastTask(taskId, existing -> {
            TimeseriesForecastTask e = existing != null ? existing : new TimeseriesForecastTask();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
            }
            e.setTaskId(taskId);
            // audit fields
            LocalDateTime now = LocalDateTime.now();
            String user = request != null ? request.getUser() : null;
            if (existing == null) {
                e.setCreateTime(now);
                e.setCreateUser(user);
            } else {
                e.setCreateTime(existing.getCreateTime());
                e.setCreateUser(existing.getCreateUser());
            }
            e.setUpdateTime(now);
            e.setUpdateUser(user);
            return e;
        });

        forecastTaskMapper.insert(entity);
        syncForecastTaskToForecastService(taskId);
        return taskId;
    }

    @Override
    public List<ForecastTaskVO> listForecastTasks() {
        return listForecastTasks(null);
    }

    @Override
    public List<ForecastTaskVO> listForecastTasks(TaskQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        return memoryCache.listForecastTasks().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toVO)
                .collect(Collectors.toList());
    }

    @Override
    public void updateForecastTaskStatus(TaskStatusUpdateRequest request) {
        if (request == null || request.getTaskId() == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        TimeseriesForecastTask entity = memoryCache.computeForecastTask(request.getTaskId(), existing -> {
            TimeseriesForecastTask e = existing != null ? existing : new TimeseriesForecastTask();
            e.setTaskId(request.getTaskId());
            e.setStatus(request.getStatus());
            // audit fields
            LocalDateTime now = LocalDateTime.now();
            if (existing == null) {
                e.setCreateTime(now);
            } else {
                e.setCreateTime(existing.getCreateTime());
                e.setCreateUser(existing.getCreateUser());
            }
            e.setUpdateTime(now);
            return e;
        });

        forecastTaskMapper.updateById(entity);
        forecastGrpcClient.updateForecastTaskStatus(request.getTaskId(), request.getStatus());
    }

    private static final long MAX_FORECAST_HORIZON = 10_000;

    @Override
    public void validateForecastObjects(ForecastTaskSaveRequest request) {
        if (request == null || request.getForecastObjects() == null || request.getForecastObjects().isEmpty()) {
            throw new BusinessException("forecastObjects must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        Set<String> targetSet = request.getForecastObjects().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (targetSet.isEmpty()) {
            throw new BusinessException("forecastObjects must contain valid sequence IDs");
        }
        for (String seqId : targetSet) {
            TimeseriesInstanceConfig instance = memoryCache.getInstanceBySequenceId(seqId);
            if (instance == null) {
                throw new BusinessException("forecast target sequence not found: " + seqId);
            }
        }
        if (request.getFeatureSequenceIds() != null) {
            for (String featId : request.getFeatureSequenceIds()) {
                if (featId != null && memoryCache.getInstanceBySequenceId(featId) == null) {
                    throw new BusinessException("feature sequence not found: " + featId);
                }
                if (featId != null && targetSet.contains(featId)) {
                    throw new BusinessException("feature sequence cannot be same as forecast target: " + featId);
                }
            }
        }
        if (request.getConstraintIds() != null && !request.getConstraintIds().isEmpty()) {
            cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
            for (String constraintId : request.getConstraintIds()) {
                TimeseriesConstraint constraint = memoryCache.getConstraint(constraintId).orElse(null);
                if (constraint == null) {
                    throw new BusinessException("constraint not found: " + constraintId);
                }
                if (!"ENABLE".equalsIgnoreCase(constraint.getEffectiveStatus())
                        || !"CONFIRMED".equalsIgnoreCase(constraint.getConfirmStatus())) {
                    throw new BusinessException("constraint not active: " + constraintId);
                }
            }
        }
        if (request.getObservationWindowMs() != null && request.getObservationWindowMs() <= 0) {
            throw new BusinessException("observationWindowMs must be positive");
        }
        if (request.getMinimumPoints() != null && request.getMinimumPoints() <= 0) {
            throw new BusinessException("minimumPoints must be positive");
        }
        if (request.getModelKey() == null || request.getModelKey().isBlank()) {
            throw new BusinessException("modelKey must not be empty");
        }
    }

    @Override
    public void validateForecastHorizon(String forecastHorizon) {
        if (forecastHorizon == null || forecastHorizon.isBlank()) {
            throw new BusinessException("forecastHorizon must not be empty");
        }
        long horizon;
        try {
            horizon = Long.parseLong(forecastHorizon.trim());
        } catch (NumberFormatException e) {
            throw new BusinessException("forecastHorizon must be a valid integer: " + forecastHorizon);
        }
        if (horizon <= 0) {
            throw new BusinessException("forecastHorizon must be positive: " + horizon);
        }
        if (horizon > MAX_FORECAST_HORIZON) {
            throw new BusinessException("forecastHorizon exceeds maximum allowed: " + horizon
                    + " (max " + MAX_FORECAST_HORIZON + ")");
        }
    }

    @Override
    public void syncForecastTaskToForecastService(String taskId) {
        if (taskId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        memoryCache.getForecastTask(taskId).ifPresent(forecastGrpcClient::syncForecastTask);
    }

    private String generateTaskId() {
        return UUID.randomUUID().toString();
    }

    private ForecastTaskVO toVO(TimeseriesForecastTask entity) {
        ForecastTaskVO vo = new ForecastTaskVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private boolean matches(TaskQueryRequest request, TimeseriesForecastTask entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getTaskId(), entity.getTaskId())
                && taskTypeMatches(request.getTaskType())
                && containsIfPresent(request.getTaskName(), entity.getTaskName())
                && equalsTextIfPresent(request.getStatus(), entity.getStatus())
                && matchesKeyword(request.getKeyword(), entity);
    }

    private boolean taskTypeMatches(String taskType) {
        return taskType == null || "FORECAST".equalsIgnoreCase(taskType);
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

    private boolean matchesKeyword(String keyword, TimeseriesForecastTask entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getTaskName())
                || containsIfPresent(keyword, entity.getForecastHorizon())
                || containsIfPresent(keyword, entity.getWarningRule());
    }
}

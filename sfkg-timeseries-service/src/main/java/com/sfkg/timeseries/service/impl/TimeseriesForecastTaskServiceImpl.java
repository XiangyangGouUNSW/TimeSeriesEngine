package com.sfkg.timeseries.service.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.DownstreamSyncValidator;
import com.sfkg.timeseries.auth.CurrentAuditUser;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.common.SemanticId;
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
        if (request == null) {
            throw new BusinessException("forecast task request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        if (request.getTaskName() != null && memoryCache.listForecastTasks(request.getProjectId()).stream()
                .anyMatch(item -> request.getTaskName().equalsIgnoreCase(item.getTaskName()))) {
            throw new BusinessException("forecast taskName already exists: " + request.getTaskName());
        }
        String taskId = request != null ? request.getTaskId() : null;
        if (taskId != null) {
            cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
            if (memoryCache.getForecastTask(request.getProjectId(), taskId).isPresent()) {
                throw new BusinessException("forecast task already exists: " + taskId);
            }
        }
        return doSaveForecastTask(request);
    }

    @Override
    public String saveForecastTask(ForecastTaskSaveRequest request) {
        requireExistingTask(request);
        return doSaveForecastTask(request);
    }

    private String doSaveForecastTask(ForecastTaskSaveRequest request) {
        if (request == null) {
            throw new BusinessException("forecast task request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        requireTaskName(request.getTaskName());
        request.setStatus(normalizeTaskStatus(request.getStatus()));
        validateForecastObjects(request);
        validateForecastHorizon(request.getForecastHorizon());
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        String taskId = request.getTaskId() == null
                ? generateTaskId(request)
                : request.getTaskId();
        ensureUniqueTaskName(request.getProjectId(), request.getTaskName(), taskId);
        TimeseriesForecastTask previous = snapshotTask(memoryCache
                .getForecastTask(request.getProjectId(), taskId).orElse(null));

        String user = CurrentAuditUser.username();
        TimeseriesForecastTask entity = memoryCache.computeForecastTask(
                request.getProjectId(), taskId, existing -> {
            TimeseriesForecastTask e = existing != null ? existing : new TimeseriesForecastTask();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
            }
            e.setTaskId(taskId);
            e.setProjectId(request.getProjectId());
            // audit fields
            LocalDateTime now = LocalDateTime.now();
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

        try {
            syncForecastTaskToForecastService(entity.getProjectId(), taskId);
        } catch (RuntimeException exception) {
            restoreTaskCache(entity.getProjectId(), taskId, previous);
            throw exception;
        }
        forecastTaskMapper.insert(entity);
        return taskId;
    }

    @Override
    public List<ForecastTaskVO> listForecastTasks() {
        return listForecastTasks(null);
    }

    @Override
    public List<ForecastTaskVO> listForecastTasks(TaskQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        List<TimeseriesForecastTask> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listForecastTasks(request.getProjectId())
                : memoryCache.listForecastTasks();
        return source.stream()
                .filter(entity -> matches(request, entity))
                .map(this::toVO)
                .collect(Collectors.toList());
    }

    @Override
    public void updateForecastTaskStatus(TaskStatusUpdateRequest request) {
        if (request == null || request.getTaskId() == null || request.getTaskId().isBlank()) {
            throw new BusinessException("taskId must not be empty");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        request.setStatus(requireTaskStatus(request.getStatus()));
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        TimeseriesForecastTask previous = snapshotTask(memoryCache
                .getForecastTask(request.getProjectId(), request.getTaskId()).orElse(null));
        if (previous == null) {
            throw new BusinessException("forecast task not found: " + request.getTaskId());
        }
        String user = CurrentAuditUser.username();
        TimeseriesForecastTask entity = memoryCache.computeForecastTask(
                request.getProjectId(), request.getTaskId(), existing -> {
            TimeseriesForecastTask e = existing != null ? existing : new TimeseriesForecastTask();
            e.setTaskId(request.getTaskId());
            e.setProjectId(request.getProjectId());
            e.setStatus(request.getStatus());
            // audit fields
            LocalDateTime now = LocalDateTime.now();
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

        try {
            DownstreamSyncValidator.requireSuccess("Analysis",
                    forecastGrpcClient.updateForecastTaskStatus(entity.getProjectId(), request.getTaskId(), request.getStatus()));
        } catch (RuntimeException exception) {
            restoreTaskCache(entity.getProjectId(), request.getTaskId(), previous);
            throw exception;
        }
        forecastTaskMapper.updateById(entity);
    }

    private static final long MAX_FORECAST_HORIZON = 10_000;

    @Override
    public void validateForecastObjects(ForecastTaskSaveRequest request) {
        if (request == null || request.getForecastObjects() == null || request.getForecastObjects().isEmpty()) {
            throw new BusinessException("forecastObjects must not be empty");
        }
        requireUniqueIdentifiers(request.getForecastObjects(), "forecastObjects");
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        Set<String> targetSet = request.getForecastObjects().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (targetSet.isEmpty()) {
            throw new BusinessException("forecastObjects must contain valid sequence IDs");
        }
        for (String seqId : targetSet) {
            TimeseriesInstanceConfig instance = memoryCache.getInstanceBySequenceId(request.getProjectId(), seqId);
            if (instance == null) {
                throw new BusinessException("forecast target sequence not found: " + seqId);
            }
        }
        if (request.getFeatureSequenceIds() != null) {
            requireUniqueIdentifiers(request.getFeatureSequenceIds(), "featureSequenceIds");
            for (String featId : request.getFeatureSequenceIds()) {
                if (featId != null && memoryCache.getInstanceBySequenceId(request.getProjectId(), featId) == null) {
                    throw new BusinessException("feature sequence not found: " + featId);
                }
                if (featId != null && targetSet.contains(featId)) {
                    throw new BusinessException("feature sequence cannot be same as forecast target: " + featId);
                }
            }
        }
        if (request.getConstraintIds() != null && !request.getConstraintIds().isEmpty()) {
            requireUniqueIdentifiers(request.getConstraintIds(), "constraintIds");
            cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
            for (String constraintId : request.getConstraintIds()) {
                TimeseriesConstraint constraint = memoryCache.getConstraint(request.getProjectId(), constraintId).orElse(null);
                if (constraint == null) {
                    throw new BusinessException("constraint not found: " + constraintId);
                }
                if (!isConstraintActive(constraint)) {
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

    private boolean isConstraintActive(TimeseriesConstraint constraint) {
        if (constraint == null) {
            return false;
        }
        return ("ENABLE".equalsIgnoreCase(constraint.getEffectiveStatus())
                || "ENABLED".equalsIgnoreCase(constraint.getEffectiveStatus()))
                && "CONFIRMED".equalsIgnoreCase(constraint.getConfirmStatus());
    }

    private void requireTaskName(String taskName) {
        if (taskName == null || taskName.isBlank()) {
            throw new BusinessException("taskName must not be empty");
        }
    }

    private String requireTaskStatus(String status) {
        String normalized = normalizeTaskStatus(status);
        if (normalized == null) {
            throw new BusinessException("task status must not be empty");
        }
        return normalized;
    }

    private String normalizeTaskStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        return switch (status.trim().toUpperCase()) {
            case "ENABLE", "ENABLED" -> "ENABLED";
            case "DISABLE", "DISABLED" -> "DISABLED";
            case "ERROR" -> "ERROR";
            default -> throw new BusinessException("unsupported task status: " + status
                    + " (expected ENABLED/DISABLED/ERROR)");
        };
    }

    private void requireUniqueIdentifiers(List<String> values, String fieldName) {
        Set<String> identifiers = new java.util.HashSet<>();
        for (String value : values) {
            if (value == null || value.isBlank()) {
                throw new BusinessException(fieldName + " must not contain empty values");
            }
            if (!identifiers.add(value)) {
                throw new BusinessException(fieldName + " contains duplicate value: " + value);
            }
        }
    }

    private void ensureUniqueTaskName(String projectId, String taskName, String taskId) {
        boolean duplicated = memoryCache.listForecastTasks(projectId).stream()
                .anyMatch(item -> !Objects.equals(taskId, item.getTaskId())
                        && taskName.equalsIgnoreCase(item.getTaskName()));
        if (duplicated) {
            throw new BusinessException("forecast taskName already exists: " + taskName);
        }
    }

    private void requireExistingTask(ForecastTaskSaveRequest request) {
        if (request == null || request.getTaskId() == null || request.getTaskId().isBlank()) {
            throw new BusinessException("taskId must be provided when updating a forecast task");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        if (memoryCache.getForecastTask(request.getProjectId(), request.getTaskId()).isEmpty()) {
            throw new BusinessException("forecast task not found: " + request.getTaskId());
        }
    }

    private TimeseriesForecastTask snapshotTask(TimeseriesForecastTask source) {
        if (source == null) {
            return null;
        }
        TimeseriesForecastTask snapshot = new TimeseriesForecastTask();
        BeanUtils.copyProperties(source, snapshot);
        snapshot.setForecastObjects(source.getForecastObjects() == null ? null : List.copyOf(source.getForecastObjects()));
        snapshot.setFeatureSequenceIds(source.getFeatureSequenceIds() == null
                ? null : List.copyOf(source.getFeatureSequenceIds()));
        snapshot.setConstraintIds(source.getConstraintIds() == null ? null : List.copyOf(source.getConstraintIds()));
        return snapshot;
    }

    private void restoreTaskCache(String projectId, String taskId, TimeseriesForecastTask previous) {
        List<TimeseriesForecastTask> restored = new java.util.ArrayList<>(memoryCache.listForecastTasks());
        restored.removeIf(item -> Objects.equals(projectId, item.getProjectId())
                && Objects.equals(taskId, item.getTaskId()));
        if (previous != null) {
            restored.add(previous);
        }
        memoryCache.replaceForecastTasks(restored);
    }

    @Override
    public void syncForecastTaskToForecastService(String taskId) {
        if (taskId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        memoryCache.getForecastTask(taskId).ifPresent(task ->
                DownstreamSyncValidator.requireSuccess("Analysis", forecastGrpcClient.syncForecastTask(task)));
    }

    private void syncForecastTaskToForecastService(String projectId, String taskId) {
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        memoryCache.getForecastTask(projectId, taskId).ifPresent(task ->
                DownstreamSyncValidator.requireSuccess("Analysis", forecastGrpcClient.syncForecastTask(task)));
    }

    private String generateTaskId(ForecastTaskSaveRequest request) {
        return SemanticId.generate(
                request == null ? null : request.getTaskName(),
                request != null && request.getForecastObjects() != null && !request.getForecastObjects().isEmpty()
                        ? request.getForecastObjects().get(0) : null);
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
        return equalsIfPresent(request.getProjectId(), entity.getProjectId())
                && equalsIfPresent(request.getTaskId(), entity.getTaskId())
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

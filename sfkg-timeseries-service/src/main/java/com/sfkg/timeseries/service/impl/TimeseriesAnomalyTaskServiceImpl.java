package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.AnomalyTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.mapper.TimeseriesAnomalyTaskMapper;
import com.sfkg.timeseries.service.TimeseriesAnomalyTaskService;
import com.sfkg.timeseries.vo.AnomalyTaskVO;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesAnomalyTaskServiceImpl implements TimeseriesAnomalyTaskService {

    private final TimeseriesAnomalyTaskMapper anomalyTaskMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final AnomalyGrpcClient anomalyGrpcClient;

    public TimeseriesAnomalyTaskServiceImpl(
            TimeseriesAnomalyTaskMapper anomalyTaskMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesCoreGrpcClient coreGrpcClient,
            AnomalyGrpcClient anomalyGrpcClient) {
        this.anomalyTaskMapper = anomalyTaskMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.coreGrpcClient = coreGrpcClient;
        this.anomalyGrpcClient = anomalyGrpcClient;
    }

    @Override
    public String createAnomalyTask(AnomalyTaskSaveRequest request) {
        String taskId = request != null ? request.getTaskId() : null;
        if (taskId != null) {
            cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
            if (memoryCache.getAnomalyTask(taskId).isPresent()) {
                throw new BusinessException("anomaly task already exists: " + taskId);
            }
        }
        return doSaveAnomalyTask(request);
    }

    @Override
    public String saveAnomalyTask(AnomalyTaskSaveRequest request) {
        return doSaveAnomalyTask(request);
    }

    private String doSaveAnomalyTask(AnomalyTaskSaveRequest request) {
        validateDetectObjects(request);
        if (request.getMethods() != null) {
            for (String method : request.getMethods()) {
                validateDetectMethod(method);
            }
        }
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        String taskId = request == null || request.getTaskId() == null
                ? generateTaskId()
                : request.getTaskId();

        TimeseriesAnomalyTask entity = memoryCache.getAnomalyTask(taskId)
                .orElseGet(TimeseriesAnomalyTask::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
        }
        entity.setTaskId(taskId);

        anomalyTaskMapper.insert(entity);
        memoryCache.putAnomalyTask(entity);
        // syncAnomalyTaskToCore(taskId);  // C端 SyncTaskStatus 暂不启用
        syncAnomalyTaskToAnomalyService(taskId);
        return taskId;
    }

    @Override
    public List<AnomalyTaskVO> listAnomalyTasks() {
        return listAnomalyTasks(null);
    }

    @Override
    public List<AnomalyTaskVO> listAnomalyTasks(TaskQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        return memoryCache.listAnomalyTasks().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toVO)
                .collect(Collectors.toList());
    }

    @Override
    public void updateAnomalyTaskStatus(TaskStatusUpdateRequest request) {
        if (request == null || request.getTaskId() == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        TimeseriesAnomalyTask entity = memoryCache.getAnomalyTask(request.getTaskId())
                .orElseGet(TimeseriesAnomalyTask::new);
        entity.setTaskId(request.getTaskId());
        entity.setStatus(request.getStatus());

        anomalyTaskMapper.updateById(entity);
        memoryCache.putAnomalyTask(entity);
        // syncAnomalyTaskToCore(request.getTaskId());  // C端 SyncTaskStatus 暂不启用
        anomalyGrpcClient.updateAnomalyTaskStatus(request.getTaskId(), request.getStatus());
    }

    private static final Set<String> VALID_DETECT_METHODS = Set.of(
            "CONSTRAINT_CHECK", "MODEL_ANOMALY_DETECTION", "HYBRID");

    @Override
    public void validateDetectObjects(AnomalyTaskSaveRequest request) {
        if (request == null || request.getSequenceIds() == null || request.getSequenceIds().isEmpty()) {
            throw new BusinessException("sequenceIds must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        for (String sequenceId : request.getSequenceIds()) {
            TimeseriesInstanceConfig instance = memoryCache.getInstanceBySequenceId(sequenceId);
            if (instance == null) {
                throw new BusinessException("sequence not found: " + sequenceId);
            }
            if (instance.getCategoryId() == null) {
                throw new BusinessException("sequence has no category: " + sequenceId);
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
                    throw new BusinessException("constraint not active: " + constraintId
                            + " effective=" + constraint.getEffectiveStatus()
                            + " confirmed=" + constraint.getConfirmStatus());
                }
            }
        }
        if (request.getContextLength() != null && request.getContextLength() <= 0) {
            throw new BusinessException("contextLength must be positive");
        }
        if (request.getSlideStepMs() != null && request.getSlideStepMs() <= 0) {
            throw new BusinessException("slideStepMs must be positive");
        }
        if (request.getMinimumPoints() != null && request.getMinimumPoints() <= 0) {
            throw new BusinessException("minimumPoints must be positive");
        }
    }

    @Override
    public void validateDetectMethod(String detectMethod) {
        if (detectMethod == null || detectMethod.isBlank()) {
            throw new BusinessException("detectMethod must not be empty");
        }
        List<String> methods = Arrays.stream(detectMethod.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .toList();
        if (methods.isEmpty()) {
            throw new BusinessException("detectMethod must not be empty");
        }
        for (String method : methods) {
            if (!VALID_DETECT_METHODS.contains(method.toUpperCase())) {
                throw new BusinessException("unsupported detectMethod: " + method
                        + ". Supported: " + VALID_DETECT_METHODS);
            }
        }
    }

    @Override
    public void syncAnomalyTaskToCore(String taskId) {
        if (taskId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        memoryCache.getAnomalyTask(taskId).ifPresent(coreGrpcClient::syncAnomalyTaskConfig);
    }

    @Override
    public void syncAnomalyTaskToAnomalyService(String taskId) {
        if (taskId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        memoryCache.getAnomalyTask(taskId).ifPresent(anomalyGrpcClient::syncAnomalyTask);
    }

    private String generateTaskId() {
        return UUID.randomUUID().toString();
    }

    private AnomalyTaskVO toVO(TimeseriesAnomalyTask entity) {
        AnomalyTaskVO vo = new AnomalyTaskVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private boolean matches(TaskQueryRequest request, TimeseriesAnomalyTask entity) {
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
        return taskType == null || "ANOMALY".equalsIgnoreCase(taskType);
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

    private boolean matchesKeyword(String keyword, TimeseriesAnomalyTask entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getTaskName())
                || containsIfPresent(keyword, entity.getWarningRule());
    }
}

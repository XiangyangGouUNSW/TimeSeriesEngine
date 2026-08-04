package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.dto.ForecastTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.mapper.TimeseriesForecastTaskMapper;
import com.sfkg.timeseries.service.TimeseriesForecastTaskService;
import com.sfkg.timeseries.vo.ForecastTaskVO;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesForecastTaskServiceImpl implements TimeseriesForecastTaskService {

    private final TimeseriesForecastTaskMapper forecastTaskMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final ForecastGrpcClient forecastGrpcClient;

    public TimeseriesForecastTaskServiceImpl(
            TimeseriesForecastTaskMapper forecastTaskMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesCoreGrpcClient coreGrpcClient,
            ForecastGrpcClient forecastGrpcClient) {
        this.forecastTaskMapper = forecastTaskMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.coreGrpcClient = coreGrpcClient;
        this.forecastGrpcClient = forecastGrpcClient;
    }

    @Override
    public Integer saveForecastTask(ForecastTaskSaveRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        Integer taskId = request == null || request.getTaskId() == null
                ? generateTaskId()
                : request.getTaskId();

        TimeseriesForecastTask entity = memoryCache.getForecastTask(taskId)
                .orElseGet(TimeseriesForecastTask::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
        }
        entity.setTaskId(taskId);

        forecastTaskMapper.insert(entity);
        memoryCache.putForecastTask(entity);
        syncForecastTaskToCore(taskId);
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
        TimeseriesForecastTask entity = memoryCache.getForecastTask(request.getTaskId())
                .orElseGet(TimeseriesForecastTask::new);
        entity.setTaskId(request.getTaskId());
        entity.setStatus(request.getStatus());

        forecastTaskMapper.updateById(entity);
        memoryCache.putForecastTask(entity);
        syncForecastTaskToCore(request.getTaskId());
        syncForecastTaskToForecastService(request.getTaskId());
    }

    @Override
    public void validateForecastObjects(ForecastTaskSaveRequest request) {
        // TODO: Restore forecast object validation here.
    }

    @Override
    public void validateForecastHorizon(String forecastHorizon) {
        // TODO: Restore forecast horizon validation here.
    }

    @Override
    public void syncForecastTaskToCore(Integer taskId) {
        if (taskId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        memoryCache.getForecastTask(taskId).ifPresent(coreGrpcClient::syncForecastTaskConfig);
    }

    @Override
    public void syncForecastTaskToForecastService(Integer taskId) {
        if (taskId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        memoryCache.getForecastTask(taskId).ifPresent(forecastGrpcClient::syncForecastTask);
    }

    private Integer generateTaskId() {
        return ThreadLocalRandom.current().nextInt(100000, 1000000);
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

    private boolean equalsIfPresent(Integer expected, Integer actual) {
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

package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigSaveRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.mapper.TimeseriesInstanceConfigMapper;
import com.sfkg.timeseries.service.TimeseriesInstanceService;
import com.sfkg.timeseries.vo.InstanceConfigVO;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesInstanceServiceImpl implements TimeseriesInstanceService {

    private final TimeseriesInstanceConfigMapper instanceConfigMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesCoreGrpcClient coreGrpcClient;

    public TimeseriesInstanceServiceImpl(
            TimeseriesInstanceConfigMapper instanceConfigMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesCoreGrpcClient coreGrpcClient) {
        this.instanceConfigMapper = instanceConfigMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.coreGrpcClient = coreGrpcClient;
    }

    @Override
    public String saveInstanceConfig(InstanceConfigSaveRequest request) {
        return doSaveInstanceConfig(request, true);
    }

    public String createInstanceConfig(InstanceConfigSaveRequest request) {
        String sequenceId = request != null ? request.getSequenceId() : null;
        if (sequenceId != null) {
            cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
            if (memoryCache.getInstanceConfig(sequenceId).isPresent()) {
                throw new BusinessException("instance already exists: " + sequenceId);
            }
        }
        return doSaveInstanceConfig(request, false);
    }

    private String doSaveInstanceConfig(InstanceConfigSaveRequest request, boolean isUpdate) {
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        String sequenceId = request == null || request.getSequenceId() == null
                ? generateSequenceId()
                : request.getSequenceId();

        TimeseriesInstanceConfig entity = memoryCache.getInstanceConfig(sequenceId)
                .orElseGet(TimeseriesInstanceConfig::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
        }
        entity.setSequenceId(sequenceId);
        if (entity.getId() == null) {
            entity.setId(sequenceId);
        }
        entity.setCategoryName(resolveCategoryName(entity.getCategoryId()));
        entity.setDeviceInstanceName(resolveDeviceInstanceName(entity.getDeviceInstanceId()));

        instanceConfigMapper.insert(entity);
        memoryCache.putInstanceConfig(entity);
        coreGrpcClient.syncInstanceConfig(entity);
        return sequenceId;
    }

    @Override
    public List<InstanceConfigVO> queryInstanceConfigs(InstanceConfigQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        return memoryCache.listInstanceConfigs().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toVO)
                .collect(Collectors.toList());
    }

    @Override
    public void validateCategory(String categoryId) {
        if (categoryId == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        memoryCache.getCategory(categoryId).orElse(null);
    }

    @Override
    public void validateDeviceInstance(String deviceInstanceId) {
        // TODO: Restore device instance validation against device service when the integration is available.
    }

    @Override
    public String generateSequenceId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void syncInstanceToGraph(String sequenceId) {
        // TODO: Restore graph synchronization here.
    }

    @Override
    public void syncInstanceToCore(String sequenceId) {
        if (sequenceId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        memoryCache.getInstanceConfig(sequenceId).ifPresent(coreGrpcClient::syncInstanceConfig);
    }

    private String resolveCategoryName(String categoryId) {
        if (categoryId == null) {
            return null;
        }
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        return memoryCache.getCategory(categoryId)
                .map(TimeseriesCategory::getCategoryName)
                .orElse(null);
    }

    private String resolveDeviceInstanceName(String deviceInstanceId) {
        return deviceInstanceId == null ? null : "device-" + deviceInstanceId;
    }

    private InstanceConfigVO toVO(TimeseriesInstanceConfig entity) {
        InstanceConfigVO vo = new InstanceConfigVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private boolean matches(InstanceConfigQueryRequest request, TimeseriesInstanceConfig entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getSequenceId(), entity.getSequenceId())
                && equalsIfPresent(request.getCategoryId(), entity.getCategoryId())
                && equalsIfPresent(request.getDeviceInstanceId(), entity.getDeviceInstanceId())
                && equalsTextIfPresent(request.getAccessStatus(), entity.getAccessStatus());
    }

    private boolean equalsIfPresent(String expected, String actual) {
        return expected == null || Objects.equals(expected, actual);
    }

    private boolean equalsTextIfPresent(String expected, String actual) {
        return expected == null || (actual != null && expected.equalsIgnoreCase(actual));
    }
}

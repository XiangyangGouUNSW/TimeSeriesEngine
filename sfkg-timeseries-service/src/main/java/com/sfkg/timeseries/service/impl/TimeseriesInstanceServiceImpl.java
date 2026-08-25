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
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.SemanticId;
import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.mapper.TimeseriesInstanceConfigMapper;
import com.sfkg.timeseries.service.TimeseriesInstanceService;
import com.sfkg.timeseries.vo.InstanceConfigVO;

@Service
public class TimeseriesInstanceServiceImpl implements TimeseriesInstanceService {

    private static final Set<String> VALID_DATA_TYPES = Set.of("double", "int64", "bool", "string");

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
        return doSaveInstanceConfig(request);
    }

    public String createInstanceConfig(InstanceConfigSaveRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        // check duplicate by instanceName if provided
        if (request != null && request.getInstanceName() != null && !request.getInstanceName().isBlank()) {
            boolean dup = memoryCache.listInstanceConfigs().stream()
                    .anyMatch(e -> Objects.equals(request.getProjectId(), e.getProjectId())
                            && request.getInstanceName().equals(e.getInstanceName()));
            if (dup) {
                throw new BusinessException("instance already exists: " + request.getInstanceName());
            }
        }
        // sequenceId optional — auto-generate if not provided
        return doSaveInstanceConfig(request);
    }

    private String doSaveInstanceConfig(InstanceConfigSaveRequest request) {
        if (request == null) {
            throw new BusinessException("instance config request must not be null");
        }
        if (request.getSequenceId() != null && request.getSequenceId().isBlank()) {
            throw new BusinessException("sequenceId must not be blank");
        }
        if (request.getInstanceName() == null || request.getInstanceName().isBlank()) {
            throw new BusinessException("instanceName must not be empty");
        }
        if (request.getCategoryId() == null || request.getCategoryId().isBlank()) {
            throw new BusinessException("categoryId must not be empty");
        }
        if (request.getDataType() == null || request.getDataType().isBlank()) {
            throw new BusinessException("dataType must not be empty");
        }
        if (!VALID_DATA_TYPES.contains(request.getDataType().trim().toLowerCase())) {
            throw new BusinessException("unsupported dataType: " + request.getDataType()
                    + " (expected double/int64/bool/string)");
        }
        validateCategory(request.getProjectId(), request.getCategoryId());
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        String sequenceId = request.getSequenceId() == null
                ? generateSequenceId(request)
                : request.getSequenceId();

        TimeseriesInstanceConfig entity = memoryCache.computeInstanceConfig(
                request != null ? request.getProjectId() : null, sequenceId, existing -> {
            TimeseriesInstanceConfig e = existing != null ? existing : new TimeseriesInstanceConfig();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
            }
            e.setSequenceId(sequenceId);
            e.setProjectId(request != null ? request.getProjectId() : (existing != null ? existing.getProjectId() : null));
            e.setCategoryName(resolveCategoryName(e.getProjectId(), e.getCategoryId()));
            e.setDeviceInstanceName(resolveDeviceInstanceName(e.getDeviceInstanceId()));
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

        instanceConfigMapper.insert(entity);
        coreGrpcClient.syncInstanceConfig(entity);

        // Re-sync relations that reference this instance's category (for category-level expansion)
        if (entity.getCategoryId() != null) {
            cacheManager.ensureTableLoaded(CachedTable.RELATION);
            for (TimeseriesRelation rel : memoryCache.listRelations().stream()
                    .filter(rel -> Objects.equals(entity.getProjectId(), rel.getProjectId())).toList()) {
                boolean matches = entity.getCategoryId().equals(rel.getTargetSequenceId());
                if (!matches && rel.getSourceSequences() != null) {
                    matches = rel.getSourceSequences().contains(entity.getCategoryId());
                }
                if (matches) {
                    coreGrpcClient.syncRelationConfig(rel);
                }
            }
            // Re-sync constraints whose variableMapping references this categoryId
            cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
            for (TimeseriesConstraint c : memoryCache.listConstraints().stream()
                    .filter(c -> Objects.equals(entity.getProjectId(), c.getProjectId())).toList()) {
                if (c.getVariableMapping() != null && c.getVariableMapping().containsValue(entity.getCategoryId())) {
                    coreGrpcClient.syncConstraintConfig(c);
                }
            }
        }

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
        validateCategory(null, categoryId);
    }

    private void validateCategory(String projectId, String categoryId) {
        if (categoryId == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        if (memoryCache.getCategory(projectId, categoryId).isEmpty()
                && memoryCache.getCategory(categoryId).isEmpty()) {
            throw new BusinessException("category not found: " + categoryId);
        }
    }

    @Override
    public void validateDeviceInstance(String deviceInstanceId) {
        // TODO: Restore device instance validation against device service when the integration is available.
    }

    @Override
    public String generateSequenceId() {
        return SemanticId.generate("sequence");
    }

    private String generateSequenceId(InstanceConfigSaveRequest request) {
        return SemanticId.generate(
                request == null ? null : request.getCategoryId(),
                request == null ? null : request.getDeviceInstanceId(),
                request == null ? null : request.getExternalSequenceId());
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

    private String resolveCategoryName(String projectId, String categoryId) {
        if (categoryId == null) {
            return null;
        }
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        return memoryCache.getCategory(projectId, categoryId)
                .or(() -> memoryCache.getCategory(categoryId))
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
        return equalsIfPresent(request.getProjectId(), entity.getProjectId())
                && equalsIfPresent(request.getSequenceId(), entity.getSequenceId())
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

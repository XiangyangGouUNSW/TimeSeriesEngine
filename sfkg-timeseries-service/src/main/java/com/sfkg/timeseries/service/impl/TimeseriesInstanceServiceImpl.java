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
import com.sfkg.timeseries.common.DownstreamSyncValidator;
import com.sfkg.timeseries.auth.CurrentAuditUser;
import com.sfkg.timeseries.common.ProjectIdValidator;
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
    private static final Set<String> VALID_SERIES_KINDS = Set.of("CONTINUOUS", "DISCRETE", "CATEGORICAL");
    private static final Set<String> VALID_ACCESS_STATUSES = Set.of("ENABLE", "DISABLE", "ENABLED", "DISABLED");

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
        requireExistingInstance(request);
        return doSaveInstanceConfig(request);
    }

    public String createInstanceConfig(InstanceConfigSaveRequest request) {
        if (request == null) {
            throw new BusinessException("instance config request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
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
        if (request.getSequenceId() != null && !request.getSequenceId().isBlank()
                && memoryCache.getInstanceBySequenceId(request.getProjectId(), request.getSequenceId()) != null) {
            throw new BusinessException("sequenceId already exists: " + request.getSequenceId());
        }
        // sequenceId optional — auto-generate if not provided
        return doSaveInstanceConfig(request);
    }

    private String doSaveInstanceConfig(InstanceConfigSaveRequest request) {
        if (request == null) {
            throw new BusinessException("instance config request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
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
        TimeseriesCategory category = memoryCache.getCategory(request.getProjectId(), request.getCategoryId())
                .orElseThrow(() -> new BusinessException("category not found: " + request.getCategoryId()));
        if (!"CONFIRMED".equalsIgnoreCase(category.getConfirmStatus())) {
            throw new BusinessException("instance category must be CONFIRMED: " + request.getCategoryId());
        }
        if (!request.getDataType().trim().equalsIgnoreCase(category.getDataType())) {
            throw new BusinessException("instance dataType must match category dataType: " + request.getCategoryId());
        }
        validateOptionalEnum("seriesKind", request.getSeriesKind(), VALID_SERIES_KINDS);
        validateOptionalEnum("accessStatus", request.getAccessStatus(), VALID_ACCESS_STATUSES);
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        String sequenceId = request.getSequenceId() == null
                ? generateSequenceId(request)
                : request.getSequenceId();
        TimeseriesInstanceConfig previous = snapshotInstance(memoryCache
                .getInstanceConfig(request.getProjectId(), sequenceId).orElse(null));

        String user = CurrentAuditUser.username();
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
            DownstreamSyncValidator.requireSuccess("Core", coreGrpcClient.syncInstanceConfig(entity));

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
                        DownstreamSyncValidator.requireSuccess("Core", coreGrpcClient.syncRelationConfig(rel));
                    }
                }
                // Re-sync constraints whose variableMapping references this categoryId
                cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
                for (TimeseriesConstraint c : memoryCache.listConstraints().stream()
                        .filter(c -> Objects.equals(entity.getProjectId(), c.getProjectId())).toList()) {
                    if (c.getVariableMapping() != null && c.getVariableMapping().containsValue(entity.getCategoryId())) {
                        DownstreamSyncValidator.requireSuccess("Core", coreGrpcClient.syncConstraintConfig(c));
                    }
                }
            }
        } catch (RuntimeException exception) {
            restoreInstanceCache(entity.getProjectId(), sequenceId, previous);
            throw exception;
        }
        instanceConfigMapper.insert(entity);

        return sequenceId;
    }

    @Override
    public List<InstanceConfigVO> queryInstanceConfigs(InstanceConfigQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        List<TimeseriesInstanceConfig> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listInstanceConfigs(request.getProjectId())
                : memoryCache.listInstanceConfigs();
        return source.stream()
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
        if (memoryCache.getCategory(projectId, categoryId).isEmpty()) {
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
                .map(TimeseriesCategory::getCategoryName)
                .orElse(null);
    }

    private String resolveDeviceInstanceName(String deviceInstanceId) {
        return deviceInstanceId == null ? null : "device-" + deviceInstanceId;
    }

    private void requireExistingInstance(InstanceConfigSaveRequest request) {
        if (request == null || request.getSequenceId() == null || request.getSequenceId().isBlank()) {
            throw new BusinessException("sequenceId must be provided when updating an instance");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        if (memoryCache.getInstanceConfig(request.getProjectId(), request.getSequenceId()).isEmpty()) {
            throw new BusinessException("instance not found: " + request.getSequenceId());
        }
    }

    private TimeseriesInstanceConfig snapshotInstance(TimeseriesInstanceConfig source) {
        if (source == null) {
            return null;
        }
        TimeseriesInstanceConfig snapshot = new TimeseriesInstanceConfig();
        BeanUtils.copyProperties(source, snapshot);
        return snapshot;
    }

    private void restoreInstanceCache(String projectId, String sequenceId, TimeseriesInstanceConfig previous) {
        List<TimeseriesInstanceConfig> restored = new java.util.ArrayList<>(memoryCache.listInstanceConfigs());
        restored.removeIf(item -> Objects.equals(projectId, item.getProjectId())
                && Objects.equals(sequenceId, item.getSequenceId()));
        if (previous != null) {
            restored.add(previous);
        }
        memoryCache.replaceInstanceConfigs(restored);
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

    private void validateOptionalEnum(String field, String value, Set<String> supported) {
        if (value != null && !value.isBlank() && !supported.contains(value.trim().toUpperCase())) {
            throw new BusinessException("unsupported " + field + ": " + value + ", expected " + supported);
        }
    }
}

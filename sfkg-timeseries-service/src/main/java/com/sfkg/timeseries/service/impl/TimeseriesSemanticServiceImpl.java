package com.sfkg.timeseries.service.impl;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.auth.CurrentAuditUser;
import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.DownstreamSyncValidator;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.common.SemanticId;
import com.sfkg.timeseries.dto.CategoryQueryRequest;
import com.sfkg.timeseries.dto.CategorySaveRequest;
import com.sfkg.timeseries.dto.CategoryStatusUpdateRequest;
import com.sfkg.timeseries.dto.ConstraintBatchSaveRequest;
import com.sfkg.timeseries.dto.ConstraintQueryRequest;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
import com.sfkg.timeseries.dto.RelationQueryRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.enums.RelationTypeEnum;
import com.sfkg.timeseries.mapper.TimeseriesCategoryMapper;
import com.sfkg.timeseries.mapper.TimeseriesConstraintMapper;
import com.sfkg.timeseries.mapper.TimeseriesRelationMapper;
import com.sfkg.timeseries.service.TimeseriesSemanticService;
import com.sfkg.timeseries.service.TimeseriesTaskContextResolver;
import com.sfkg.timeseries.vo.CategoryVO;
import com.sfkg.timeseries.vo.ConstraintVO;
import com.sfkg.timeseries.vo.RelationVO;

@Service
public class TimeseriesSemanticServiceImpl implements TimeseriesSemanticService {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesSemanticServiceImpl.class);

    private final TimeseriesCategoryMapper categoryMapper;
    private final TimeseriesConstraintMapper constraintMapper;
    private final TimeseriesRelationMapper relationMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final AnomalyGrpcClient anomalyGrpcClient;
    private final ForecastGrpcClient forecastGrpcClient;
    private final TimeseriesTaskContextResolver contextResolver;

    public TimeseriesSemanticServiceImpl(
            TimeseriesCategoryMapper categoryMapper,
            TimeseriesConstraintMapper constraintMapper,
            TimeseriesRelationMapper relationMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesCoreGrpcClient coreGrpcClient,
            AnomalyGrpcClient anomalyGrpcClient,
            ForecastGrpcClient forecastGrpcClient,
            TimeseriesTaskContextResolver contextResolver) {
        this.categoryMapper = categoryMapper;
        this.constraintMapper = constraintMapper;
        this.relationMapper = relationMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.coreGrpcClient = coreGrpcClient;
        this.anomalyGrpcClient = anomalyGrpcClient;
        this.forecastGrpcClient = forecastGrpcClient;
        this.contextResolver = contextResolver;
    }

    @Override
    public List<CategoryVO> listCategories(CategoryQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        List<TimeseriesCategory> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listCategories(request.getProjectId())
                : memoryCache.listCategories();
        return source.stream()
                .filter(entity -> matches(request, entity))
                .map(this::toCategoryVO)
                .collect(Collectors.toList());
    }

    @Override
    public String saveCategory(CategorySaveRequest request) {
        requireExistingCategory(request);
        return doSaveCategory(request);
    }

    public String createCategory(CategorySaveRequest request) {
        if (request == null) {
            throw new BusinessException("category request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        if (request.getCategoryName() != null && memoryCache.listCategories(request.getProjectId()).stream()
                .anyMatch(item -> request.getCategoryName().equalsIgnoreCase(item.getCategoryName()))) {
            throw new BusinessException("categoryName already exists: " + request.getCategoryName());
        }
        String categoryId = request != null ? request.getCategoryId() : null;
        if (categoryId != null) {
            cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
            if (memoryCache.getCategory(request.getProjectId(), categoryId).isPresent()) {
                throw new BusinessException("category already exists: " + categoryId);
            }
        }
        return doSaveCategory(request);
    }

    private String doSaveCategory(CategorySaveRequest request) {
        if (request == null) {
            throw new BusinessException("category request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        if (request.getCategoryName() == null || request.getCategoryName().isBlank()) {
            throw new BusinessException("categoryName must not be empty");
        }
        if (request.getDataType() == null || request.getDataType().isBlank()) {
            throw new BusinessException("dataType must not be empty");
        }
        if (!VALID_DATA_TYPES.contains(request.getDataType().trim().toLowerCase())) {
            throw new BusinessException("unsupported dataType: " + request.getDataType()
                    + " (expected double/int64/bool/string)");
        }
        request.setDataType(request.getDataType().trim().toLowerCase());
        validateConfirmStatus(request.getConfirmStatus());
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        String categoryId = request.getCategoryId() == null
                ? SemanticId.generate(request.getCategoryName())
                : request.getCategoryId();
        TimeseriesCategory currentCategory = memoryCache.getCategory(request.getProjectId(), categoryId).orElse(null);
        ensureUniqueCategoryName(request.getProjectId(), request.getCategoryName(), categoryId);
        if (currentCategory != null && !request.getDataType().equalsIgnoreCase(currentCategory.getDataType())
                && memoryCache.listInstanceConfigs(request.getProjectId()).stream()
                        .anyMatch(instance -> categoryId.equals(instance.getCategoryId()))) {
            throw new BusinessException("cannot change dataType for referenced category: " + categoryId);
        }

        String user = CurrentAuditUser.username();
        TimeseriesCategory entity = memoryCache.computeCategory(
                request != null ? request.getProjectId() : null, categoryId, existing -> {
            TimeseriesCategory e = existing != null ? existing : new TimeseriesCategory();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
            }
            e.setCategoryId(categoryId);
            e.setProjectId(request != null ? request.getProjectId() : (existing != null ? existing.getProjectId() : null));
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

        categoryMapper.insert(entity);
        return categoryId;
    }

    @Override
    public void updateCategoryStatus(CategoryStatusUpdateRequest request) {
        if (request == null || request.getCategoryId() == null || request.getCategoryId().isBlank()) {
            throw new BusinessException("categoryId must not be empty");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        validateConfirmStatus(request.getConfirmStatus());
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        if (memoryCache.getCategory(request.getProjectId(), request.getCategoryId()).isEmpty()) {
            throw new BusinessException("category not found: " + request.getCategoryId());
        }
        String user = CurrentAuditUser.username();
        TimeseriesCategory entity = memoryCache.computeCategory(
                request.getProjectId(), request.getCategoryId(), existing -> {
            TimeseriesCategory e = existing != null ? existing : new TimeseriesCategory();
            e.setCategoryId(request.getCategoryId());
            e.setProjectId(request.getProjectId());
            if (request.getConfirmStatus() != null) {
                e.setConfirmStatus(request.getConfirmStatus());
            }
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
        categoryMapper.updateById(entity);
    }

    @Override
    public List<ConstraintVO> listConstraints(ConstraintQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        List<TimeseriesConstraint> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listConstraints(request.getProjectId())
                : memoryCache.listConstraints();
        return source.stream()
                .filter(entity -> matches(request, entity))
                .map(this::toConstraintVO)
                .collect(Collectors.toList());
    }

    @Override
    public String saveConstraint(ConstraintSaveRequest request) {
        requireExistingConstraint(request);
        return doSaveConstraint(request);
    }

    public String createConstraint(ConstraintSaveRequest request) {
        if (request == null) {
            throw new BusinessException("constraint request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        if (request.getConstraintName() != null && memoryCache.listConstraints(request.getProjectId()).stream()
                .anyMatch(item -> request.getConstraintName().equalsIgnoreCase(item.getConstraintName()))) {
            throw new BusinessException("constraintName already exists: " + request.getConstraintName());
        }
        String constraintId = request != null ? request.getConstraintId() : null;
        if (constraintId != null) {
            cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
            if (memoryCache.getConstraint(request.getProjectId(), constraintId).isPresent()) {
                throw new BusinessException("constraint already exists: " + constraintId);
            }
        }
        return doSaveConstraint(request);
    }

    @Override
    public List<String> createConstraintBatch(ConstraintBatchSaveRequest request) {
        if (request == null || request.getConstraints() == null || request.getConstraints().isEmpty()) {
            throw new BusinessException("constraint batch must not be empty");
        }
        if (request.getOrGroupId() == null || request.getOrGroupId().isBlank()) {
            throw new BusinessException("orGroupId must not be empty for constraint batch");
        }
        String projectId = ProjectIdValidator.require(request.getProjectId());
        validateOrGroupId(request.getOrGroupId());
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        String user = CurrentAuditUser.username();

        List<TimeseriesConstraint> entities = new ArrayList<>();
        List<String> createdIds = new ArrayList<>();
        Set<String> seenIds = new HashSet<>();
        Set<String> seenNames = new HashSet<>();
        for (ConstraintSaveRequest member : request.getConstraints()) {
            if (member == null) {
                throw new BusinessException("constraint batch contains null member");
            }
            member.setProjectId(projectId);
            member.setOrGroupId(request.getOrGroupId());
            validateConstraintExpression(member.getConstraintExpression());
            validateVariableMapping(projectId, member.getVariableMapping());
            if (member.getTerms() == null || member.getTerms().isEmpty()) {
                throw new BusinessException("constraint terms must not be empty: " + member.getConstraintName());
            }
            validateConstraintTerms(member.getTerms());
            validateConstraintFields(member);
            validateConstraintVariableCoverage(member);
            if (!seenNames.add(member.getConstraintName().trim().toLowerCase())) {
                throw new BusinessException("duplicate constraint name in batch: " + member.getConstraintName());
            }

            String constraintId = member.getConstraintId() == null
                    ? SemanticId.generate(
                            member.getConstraintName(),
                            member.getVariableMapping() != null && !member.getVariableMapping().isEmpty()
                                    ? member.getVariableMapping().values().iterator().next() : null)
                    : member.getConstraintId();
            if (!seenIds.add(constraintId)) {
                throw new BusinessException("duplicate constraint in batch: " + constraintId);
            }
            if (memoryCache.getConstraint(projectId, constraintId).isPresent()) {
                throw new BusinessException("constraint already exists: " + constraintId);
            }
            ensureUniqueConstraintName(projectId, member.getConstraintName(), constraintId);

            TimeseriesConstraint entity = new TimeseriesConstraint();
            BeanUtils.copyProperties(member, entity);
            if (member.getTerms() != null) {
                entity.setTerms(member.getTerms().stream()
                        .map(dto -> {
                            TimeseriesConstraint.ConstraintTermItem item = new TimeseriesConstraint.ConstraintTermItem();
                            item.setVariable(dto.getVariable());
                            item.setCoefficient(dto.getCoefficient());
                            item.setSampleOffset(dto.getSampleOffset());
                            item.setAggregation(dto.getAggregation());
                            return item;
                        })
                        .collect(Collectors.toList()));
            }
            entity.setConstraintId(constraintId);
            entity.setProjectId(projectId);
            entity.setOrGroupId(request.getOrGroupId());
            LocalDateTime now = LocalDateTime.now();
            entity.setCreateTime(now);
            entity.setUpdateTime(now);
            entity.setCreateUser(user);
            entity.setUpdateUser(user);
            entities.add(entity);
            createdIds.add(constraintId);
        }

        // 先让 Core 原子确认整组规则，避免本地保存了无法执行的 OR 配置。
        DownstreamSyncValidator.requireSuccess("Core", coreGrpcClient.syncConstraintConfigs(entities));
        for (TimeseriesConstraint entity : entities) {
            constraintMapper.insert(entity);
            memoryCache.putConstraint(entity);
        }
        return createdIds;
    }

    private String doSaveConstraint(ConstraintSaveRequest request) {
        if (request == null) {
            throw new BusinessException("constraint request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        validateConstraintFields(request);
        validateConstraintExpression(request.getConstraintExpression());
        validateVariableMapping(request.getProjectId(), request.getVariableMapping());
        if (request.getTerms() == null || request.getTerms().isEmpty()) {
            throw new BusinessException("constraint terms must not be empty");
        }
        validateConstraintTerms(request.getTerms());
        validateConstraintVariableCoverage(request);
        validateOrGroupId(request.getOrGroupId());
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        String constraintId = request.getConstraintId() == null
                ? SemanticId.generate(
                        request.getConstraintName(),
                        request.getVariableMapping() != null
                                && !request.getVariableMapping().isEmpty()
                                ? request.getVariableMapping().values().iterator().next() : null)
                : request.getConstraintId();
        ensureUniqueConstraintName(request.getProjectId(), request.getConstraintName(), constraintId);

        TimeseriesConstraint previous = snapshotConstraint(
                memoryCache.getConstraint(
                        request != null ? request.getProjectId() : null, constraintId).orElse(null));

        String user = CurrentAuditUser.username();
        TimeseriesConstraint entity = memoryCache.computeConstraint(
                request != null ? request.getProjectId() : null, constraintId, existing -> {
            TimeseriesConstraint e = existing != null ? existing : new TimeseriesConstraint();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
                if (request.getTerms() != null) {
                    e.setTerms(request.getTerms().stream()
                            .map(dto -> {
                                TimeseriesConstraint.ConstraintTermItem item = new TimeseriesConstraint.ConstraintTermItem();
                                item.setVariable(dto.getVariable());
                                item.setCoefficient(dto.getCoefficient());
                                item.setSampleOffset(dto.getSampleOffset());
                                item.setAggregation(dto.getAggregation());
                                return item;
                            })
                            .collect(Collectors.toList()));
                }
            }
            e.setConstraintId(constraintId);
            e.setProjectId(request != null ? request.getProjectId() : (existing != null ? existing.getProjectId() : null));
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
            syncSemanticToCore(entity.getProjectId(), constraintId);
        } catch (RuntimeException exception) {
            restoreConstraintCache(entity.getProjectId(), constraintId, previous);
            throw exception;
        }
        constraintMapper.insert(entity);
        reSyncTasksForConstraint(entity.getProjectId(), constraintId, previous);
        return constraintId;
    }

    @Override
    public void updateConstraintStatus(ConstraintStatusUpdateRequest request) {
        if (request == null || request.getConstraintId() == null || request.getConstraintId().isBlank()) {
            throw new BusinessException("constraintId must not be empty");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        validateConfirmStatus(request.getConfirmStatus());
        validateEffectiveStatus(request.getEffectiveStatus());
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        TimeseriesConstraint previous = snapshotConstraint(memoryCache
                .getConstraint(request.getProjectId(), request.getConstraintId()).orElse(null));
        if (previous == null) {
            throw new BusinessException("constraint not found: " + request.getConstraintId());
        }
        String user = CurrentAuditUser.username();
        TimeseriesConstraint entity = memoryCache.computeConstraint(
                request.getProjectId(), request.getConstraintId(), existing -> {
            TimeseriesConstraint e = existing != null ? existing : new TimeseriesConstraint();
            e.setConstraintId(request.getConstraintId());
            e.setProjectId(request.getProjectId());
            if (request.getConfirmStatus() != null) {
                e.setConfirmStatus(request.getConfirmStatus());
            }
            if (request.getEffectiveStatus() != null) {
                e.setEffectiveStatus(request.getEffectiveStatus());
            }
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
            syncSemanticToCore(entity.getProjectId(), entity.getConstraintId());
        } catch (RuntimeException exception) {
            restoreConstraintCache(entity.getProjectId(), entity.getConstraintId(), previous);
            throw exception;
        }
        constraintMapper.updateById(entity);
        reSyncTasksForConstraint(entity.getProjectId(), entity.getConstraintId());
    }

    @Override
    public void updateConstraintStatus(String constraintId, String status) {
        if (constraintId == null) {
            return;
        }
        ConstraintStatusUpdateRequest request = new ConstraintStatusUpdateRequest();
        request.setConstraintId(constraintId);
        request.setEffectiveStatus(status);
        updateConstraintStatus(request);
    }

    @Override
    public List<RelationVO> listRelations(RelationQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        List<TimeseriesRelation> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listRelations(request.getProjectId())
                : memoryCache.listRelations();
        return source.stream()
                .filter(entity -> matches(request, entity))
                .map(this::toRelationVO)
                .collect(Collectors.toList());
    }

    @Override
    public String saveRelation(RelationSaveRequest request) {
        requireExistingRelation(request);
        return doSaveRelation(request);
    }

    public String createRelation(RelationSaveRequest request) {
        if (request == null) {
            throw new BusinessException("relation config must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        if (request.getRelationName() != null && memoryCache.listRelations(request.getProjectId()).stream()
                .anyMatch(item -> request.getRelationName().equalsIgnoreCase(item.getRelationName()))) {
            throw new BusinessException("relationName already exists: " + request.getRelationName());
        }
        String relationId = request != null ? request.getRelationId() : null;
        if (relationId != null) {
            cacheManager.ensureTableLoaded(CachedTable.RELATION);
            if (memoryCache.getRelation(request.getProjectId(), relationId).isPresent()) {
                throw new BusinessException("relation already exists: " + relationId);
            }
        }
        return doSaveRelation(request);
    }

    private String doSaveRelation(RelationSaveRequest request) {
        validateRelationConfig(request);
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        String relationId = request == null || request.getRelationId() == null
                ? SemanticId.generate(
                        request != null ? request.getRelationName() : null,
                        request != null && request.getSourceSequences() != null
                                && !request.getSourceSequences().isEmpty()
                                ? request.getSourceSequences().get(0) : null,
                        request != null ? request.getTargetSequenceId() : null,
                        request != null ? request.getRelationType() : null)
                : request.getRelationId();
        ensureUniqueRelation(request, relationId);
        TimeseriesRelation previous = snapshotRelation(memoryCache
                .getRelation(request.getProjectId(), relationId).orElse(null));

        String user = CurrentAuditUser.username();
        TimeseriesRelation entity = memoryCache.computeRelation(
                request != null ? request.getProjectId() : null, relationId, existing -> {
            TimeseriesRelation e = existing != null ? existing : new TimeseriesRelation();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
            }
            e.setRelationId(relationId);
            e.setProjectId(request != null ? request.getProjectId() : (existing != null ? existing.getProjectId() : null));
            e.setTargetCategoryName(resolveCategoryName(e.getProjectId(), e.getTargetSequenceId()));
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
            syncSemanticToCore(entity.getProjectId(), relationId);
        } catch (RuntimeException exception) {
            restoreRelationCache(entity.getProjectId(), relationId, previous);
            throw exception;
        }
        relationMapper.insert(entity);
        reSyncTasksForRelation(entity.getProjectId(), relationId);
        return relationId;
    }

    @Override
    public void updateRelationStatus(RelationStatusUpdateRequest request) {
        if (request == null || request.getRelationId() == null || request.getRelationId().isBlank()) {
            throw new BusinessException("relationId must not be empty");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        validateConfirmStatus(request.getConfirmStatus());
        validateEffectiveStatus(request.getEffectiveStatus());
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        TimeseriesRelation previous = snapshotRelation(memoryCache
                .getRelation(request.getProjectId(), request.getRelationId()).orElse(null));
        if (previous == null) {
            throw new BusinessException("relation not found: " + request.getRelationId());
        }
        String user = CurrentAuditUser.username();
        TimeseriesRelation entity = memoryCache.computeRelation(
                request.getProjectId(), request.getRelationId(), existing -> {
            TimeseriesRelation e = existing != null ? existing : new TimeseriesRelation();
            e.setRelationId(request.getRelationId());
            e.setProjectId(request.getProjectId());
            if (request.getConfirmStatus() != null) {
                e.setConfirmStatus(request.getConfirmStatus());
            }
            if (request.getEffectiveStatus() != null) {
                e.setEffectiveStatus(request.getEffectiveStatus());
            }
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
            syncSemanticToCore(entity.getProjectId(), entity.getRelationId());
        } catch (RuntimeException exception) {
            restoreRelationCache(entity.getProjectId(), entity.getRelationId(), previous);
            throw exception;
        }
        relationMapper.updateById(entity);
        reSyncTasksForRelation(entity.getProjectId(), entity.getRelationId());
    }

    @Override
    public void updateRelationStatus(String relationId, String status) {
        if (relationId == null) {
            return;
        }
        RelationStatusUpdateRequest request = new RelationStatusUpdateRequest();
        request.setRelationId(relationId);
        request.setEffectiveStatus(status);
        updateRelationStatus(request);
    }

    private static final Set<String> VALID_DATA_TYPES = Set.of("double", "int64", "bool", "string");

    private static final Set<String> VALID_CONSTRAINT_AGGREGATIONS = Set.of(
            "SAMPLE", "AVERAGE", "MAXIMUM", "MINIMUM");

    private static final Set<String> VALID_CONFIRM_STATUSES = Set.of(
            "PENDING", "CONFIRMED", "REJECTED");

    private static final Set<String> VALID_EFFECTIVE_STATUSES = Set.of(
            "ENABLE", "ENABLED", "DISABLE", "DISABLED");

    /** 校验每个 term 的变量、系数、偏移和聚合方式。 */
    private void validateConstraintTerms(List<ConstraintSaveRequest.ConstraintTermDTO> terms) {
        Set<String> variables = new HashSet<>();
        for (ConstraintSaveRequest.ConstraintTermDTO term : terms) {
            if (term == null) {
                throw new BusinessException("constraint term must not be null");
            }
            if (term.getVariable() == null || term.getVariable().isBlank()) {
                throw new BusinessException("constraint term variable must not be empty");
            }
            if (!variables.add(term.getVariable())) {
                throw new BusinessException("duplicate constraint term variable: " + term.getVariable());
            }
            if (term.getCoefficient() == null || !Double.isFinite(term.getCoefficient())) {
                throw new BusinessException("constraint term coefficient must be finite: " + term.getVariable());
            }
            if (term.getSampleOffset() != null && term.getSampleOffset() < 0) {
                throw new BusinessException("constraint term sampleOffset must not be negative: " + term.getVariable());
            }
            if (term.getAggregation() != null && !term.getAggregation().isBlank()
                    && !VALID_CONSTRAINT_AGGREGATIONS.contains(term.getAggregation().trim().toUpperCase())) {
                throw new BusinessException("unsupported constraint aggregation: " + term.getAggregation()
                        + ". Supported: " + VALID_CONSTRAINT_AGGREGATIONS);
            }
        }
    }

    private void validateConstraintFields(ConstraintSaveRequest request) {
        if (request.getConstraintName() == null || request.getConstraintName().isBlank()) {
            throw new BusinessException("constraintName must not be empty");
        }
        validateFiniteBound("lowerBound", request.getLowerBound());
        validateFiniteBound("upperBound", request.getUpperBound());
        if (request.getLowerBound() != null && request.getUpperBound() != null
                && request.getLowerBound() > request.getUpperBound()) {
            throw new BusinessException("lowerBound must not exceed upperBound");
        }
        validateEffectiveStatus(request.getEffectiveStatus());
        validateConfirmStatus(request.getConfirmStatus());
    }

    private void validateFiniteBound(String fieldName, Double value) {
        if (value != null && !Double.isFinite(value)) {
            throw new BusinessException(fieldName + " must be finite");
        }
    }

    private void validateConstraintVariableCoverage(ConstraintSaveRequest request) {
        Set<String> mappedVariables = new HashSet<>(request.getVariableMapping().keySet());
        Set<String> termVariables = request.getTerms().stream()
                .map(ConstraintSaveRequest.ConstraintTermDTO::getVariable)
                .collect(Collectors.toSet());
        if (!mappedVariables.equals(termVariables)) {
            throw new BusinessException("constraint terms must match variableMapping keys");
        }
    }

    private void validateConfirmStatus(String status) {
        validateOptionalEnum("confirmStatus", status, VALID_CONFIRM_STATUSES);
    }

    private void validateEffectiveStatus(String status) {
        validateOptionalEnum("effectiveStatus", status, VALID_EFFECTIVE_STATUSES);
    }

    private void validateOptionalEnum(String fieldName, String value, Set<String> supportedValues) {
        if (value != null && !value.isBlank() && !supportedValues.contains(value.trim().toUpperCase())) {
            throw new BusinessException("unsupported " + fieldName + ": " + value
                    + ". Supported: " + supportedValues);
        }
    }

    /** OR 组 id 只允许字母数字和 _ - . 字符，避免污染 Core 端 clause key。 */
    private void validateOrGroupId(String orGroupId) {
        if (orGroupId == null || orGroupId.isBlank()) {
            return;
        }
        String trimmed = orGroupId.trim();
        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (!Character.isLetterOrDigit(ch) && ch != '_' && ch != '-' && ch != '.') {
                throw new BusinessException("orGroupId contains invalid character '" + ch
                        + "' at position " + i);
            }
        }
    }

    @Override
    public void validateConstraintExpression(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new BusinessException("constraint expression must not be empty");
        }
        String trimmed = expression.trim();
        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (!Character.isLetterOrDigit(ch)
                    && ch != ' '
                    && ch != '_'
                    && ch != '(' && ch != ')'
                    && ch != '+' && ch != '-' && ch != '*' && ch != '/'
                    && ch != '<' && ch != '>'
                    && ch != '=' && ch != '!'
                    && ch != '&' && ch != '|'
                    && ch != '.') {
                throw new BusinessException("constraint expression contains invalid character '"
                        + ch + "' at position " + i);
            }
        }
    }

    @Override
    public void validateVariableMapping(Map<String, String> variableMapping) {
        validateVariableMapping(null, variableMapping);
    }

    private void validateVariableMapping(String projectId, Map<String, String> variableMapping) {
        if (variableMapping == null || variableMapping.isEmpty()) {
            throw new BusinessException("variable mapping must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        for (Map.Entry<String, String> entry : variableMapping.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw new BusinessException("variable name must not be empty in mapping");
            }
            if (entry.getValue() == null || entry.getValue().isBlank()) {
                throw new BusinessException("sequenceId must not be empty for variable: " + entry.getKey());
            }
            if (!isValidSequenceOrCategory(projectId, entry.getValue())) {
                throw new BusinessException("mapped sequence or category not found: " + entry.getValue()
                        + " for variable: " + entry.getKey());
            }
        }
    }

    @Override
    public void validateRelationConfig(RelationSaveRequest request) {
        if (request == null) {
            throw new BusinessException("relation config must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        if (request.getRelationName() == null || request.getRelationName().isBlank()) {
            throw new BusinessException("relationName must not be empty");
        }
        validateConfirmStatus(request.getConfirmStatus());
        validateEffectiveStatus(request.getEffectiveStatus());
        if (request.getSourceSequences() == null || request.getSourceSequences().isEmpty()) {
            throw new BusinessException("sourceSequences must not be empty");
        }
        if (request.getTargetSequenceId() == null || request.getTargetSequenceId().isBlank()) {
            throw new BusinessException("targetSequenceId must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        Set<String> sourceSet = new HashSet<>();
        for (String src : request.getSourceSequences()) {
            if (src == null || src.isBlank()) {
                throw new BusinessException("source sequence must not be empty");
            }
            if (src.equals(request.getTargetSequenceId())) {
                throw new BusinessException("source sequence cannot equal target: " + src);
            }
            if (!sourceSet.add(src)) {
                throw new BusinessException("duplicate source sequence: " + src);
            }
            if (!isValidSequenceOrCategory(request.getProjectId(), src)) {
                throw new BusinessException("source sequence or category not found: " + src);
            }
        }
        if (sourceSet.isEmpty()) {
            throw new BusinessException("sourceSequences must contain valid sequence or category IDs");
        }
        if (!isValidSequenceOrCategory(request.getProjectId(), request.getTargetSequenceId())) {
            throw new BusinessException("target sequence or category not found: " + request.getTargetSequenceId());
        }
        // relationType 必填：空值在 P 端既不算因果型、也没有互耦标记，会被静默忽略，
        // 等同于"配置了一条永不生效的关系"，因此直接拒绝。
        // 存储统一归一化为大写；下发 C 端时再转小写（C 端按 "correlation" 精确匹配）。
        RelationTypeEnum relationType = RelationTypeEnum.from(request.getRelationType());
        if (relationType == null) {
            throw new BusinessException("unsupported relationType: " + request.getRelationType()
                    + ". Supported: " + RelationTypeEnum.supportedValues());
        }
        request.setRelationType(relationType.name());
        if (request.getConfidence() != null) {
            BigDecimal conf = request.getConfidence();
            if (conf.compareTo(BigDecimal.ZERO) < 0 || conf.compareTo(BigDecimal.ONE) > 0) {
                throw new BusinessException("confidence must be between 0 and 1: " + conf);
            }
        }
        if (request.getLagRange() != null && !request.getLagRange().isBlank()) {
            String lag = request.getLagRange().trim();
            if (!lag.matches("^\\d+[mhd]?-\\d+[mhd]?$") && !lag.matches("^\\d+$")) {
                throw new BusinessException("invalid lagRange format: " + lag + ". Expected e.g. 0m-10m or 5");
            }
        }
    }

    private void ensureUniqueCategoryName(String projectId, String categoryName, String categoryId) {
        boolean duplicated = memoryCache.listCategories(projectId).stream()
                .anyMatch(item -> !Objects.equals(categoryId, item.getCategoryId())
                        && categoryName.equalsIgnoreCase(item.getCategoryName()));
        if (duplicated) {
            throw new BusinessException("categoryName already exists: " + categoryName);
        }
    }

    private void ensureUniqueConstraintName(String projectId, String constraintName, String constraintId) {
        boolean duplicated = memoryCache.listConstraints(projectId).stream()
                .anyMatch(item -> !Objects.equals(constraintId, item.getConstraintId())
                        && constraintName.equalsIgnoreCase(item.getConstraintName()));
        if (duplicated) {
            throw new BusinessException("constraintName already exists: " + constraintName);
        }
    }

    private void ensureUniqueRelation(RelationSaveRequest request, String relationId) {
        Set<String> sourceSet = new HashSet<>(request.getSourceSequences());
        boolean duplicated = memoryCache.listRelations(request.getProjectId()).stream()
                .filter(item -> !Objects.equals(relationId, item.getRelationId()))
                .anyMatch(item -> request.getRelationName().equalsIgnoreCase(item.getRelationName())
                        || (Objects.equals(request.getTargetSequenceId(), item.getTargetSequenceId())
                                && request.getRelationType().equalsIgnoreCase(item.getRelationType())
                                && sourceSet.equals(new HashSet<>(item.getSourceSequences()))));
        if (duplicated) {
            throw new BusinessException("duplicate relation name or endpoint configuration: " + request.getRelationName());
        }
    }

    private void requireExistingCategory(CategorySaveRequest request) {
        if (request == null || request.getCategoryId() == null || request.getCategoryId().isBlank()) {
            throw new BusinessException("categoryId must be provided when updating a category");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        if (memoryCache.getCategory(request.getProjectId(), request.getCategoryId()).isEmpty()) {
            throw new BusinessException("category not found: " + request.getCategoryId());
        }
    }

    private void requireExistingConstraint(ConstraintSaveRequest request) {
        if (request == null || request.getConstraintId() == null || request.getConstraintId().isBlank()) {
            throw new BusinessException("constraintId must be provided when updating a constraint");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        if (memoryCache.getConstraint(request.getProjectId(), request.getConstraintId()).isEmpty()) {
            throw new BusinessException("constraint not found: " + request.getConstraintId());
        }
    }

    private void requireExistingRelation(RelationSaveRequest request) {
        if (request == null || request.getRelationId() == null || request.getRelationId().isBlank()) {
            throw new BusinessException("relationId must be provided when updating a relation");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        if (memoryCache.getRelation(request.getProjectId(), request.getRelationId()).isEmpty()) {
            throw new BusinessException("relation not found: " + request.getRelationId());
        }
    }

    @Override
    public void syncSemanticToGraph(String semanticId) {
        // TODO: Restore graph synchronization here.
    }

    @Override
    public void syncSemanticToCore(String semanticId) {
        if (semanticId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        memoryCache.getConstraint(semanticId).ifPresent(this::syncConstraintToCore);
        memoryCache.getRelation(semanticId).ifPresent(this::syncRelationToCore);
    }

    public void syncSemanticToCore(String projectId, String semanticId) {
        if (semanticId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        memoryCache.getConstraint(projectId, semanticId).ifPresent(this::syncConstraintToCore);
        memoryCache.getRelation(projectId, semanticId).ifPresent(this::syncRelationToCore);
    }

    private void syncConstraintToCore(TimeseriesConstraint constraint) {
        SyncResult result = coreGrpcClient.syncConstraintConfig(constraint);
        DownstreamSyncValidator.requireSuccess("Core", result);
    }

    private void syncRelationToCore(TimeseriesRelation relation) {
        SyncResult result = coreGrpcClient.syncRelationConfig(relation);
        DownstreamSyncValidator.requireSuccess("Core", result);
    }

    /**
     * 约束快照：computeConstraint 会原地修改缓存中的已有对象，
     * 因此捕获「变更前」状态时必须拷贝，避免与新值同引用。
     */
    private TimeseriesConstraint snapshotConstraint(TimeseriesConstraint source) {
        if (source == null) {
            return null;
        }
        TimeseriesConstraint snapshot = new TimeseriesConstraint();
        snapshot.setProjectId(source.getProjectId());
        snapshot.setConstraintId(source.getConstraintId());
        snapshot.setConstraintName(source.getConstraintName());
        snapshot.setConstraintDescription(source.getConstraintDescription());
        snapshot.setConstraintExpression(source.getConstraintExpression());
        snapshot.setLowerBound(source.getLowerBound());
        snapshot.setUpperBound(source.getUpperBound());
        snapshot.setEffectiveStatus(source.getEffectiveStatus());
        snapshot.setConfirmStatus(source.getConfirmStatus());
        snapshot.setOrGroupId(source.getOrGroupId());
        snapshot.setVariableMapping(source.getVariableMapping() != null
                ? new java.util.LinkedHashMap<>(source.getVariableMapping()) : null);
        snapshot.setTerms(source.getTerms() != null
                ? new java.util.ArrayList<>(source.getTerms()) : null);
        snapshot.setCreateTime(source.getCreateTime());
        snapshot.setUpdateTime(source.getUpdateTime());
        snapshot.setCreateUser(source.getCreateUser());
        snapshot.setUpdateUser(source.getUpdateUser());
        return snapshot;
    }

    private void restoreConstraintCache(String projectId, String constraintId, TimeseriesConstraint previous) {
        List<TimeseriesConstraint> restored = new ArrayList<>(memoryCache.listConstraints());
        restored.removeIf(item -> Objects.equals(projectId, item.getProjectId())
                && Objects.equals(constraintId, item.getConstraintId()));
        if (previous != null) {
            restored.add(previous);
        }
        memoryCache.replaceConstraints(restored);
    }

    private TimeseriesRelation snapshotRelation(TimeseriesRelation source) {
        if (source == null) {
            return null;
        }
        TimeseriesRelation snapshot = new TimeseriesRelation();
        BeanUtils.copyProperties(source, snapshot);
        snapshot.setSourceSequences(source.getSourceSequences() == null
                ? null : new ArrayList<>(source.getSourceSequences()));
        return snapshot;
    }

    private void restoreRelationCache(String projectId, String relationId, TimeseriesRelation previous) {
        List<TimeseriesRelation> restored = new ArrayList<>(memoryCache.listRelations());
        restored.removeIf(item -> Objects.equals(projectId, item.getProjectId())
                && Objects.equals(relationId, item.getRelationId()));
        if (previous != null) {
            restored.add(previous);
        }
        memoryCache.replaceRelations(restored);
    }

    private CategoryVO toCategoryVO(TimeseriesCategory entity) {
        CategoryVO vo = new CategoryVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private ConstraintVO toConstraintVO(TimeseriesConstraint entity) {
        ConstraintVO vo = new ConstraintVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private RelationVO toRelationVO(TimeseriesRelation entity) {
        RelationVO vo = new RelationVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
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

    private boolean matches(CategoryQueryRequest request, TimeseriesCategory entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getProjectId(), entity.getProjectId())
                && equalsIfPresent(request.getCategoryId(), entity.getCategoryId())
                && containsIfPresent(request.getCategoryName(), entity.getCategoryName())
                && equalsTextIfPresent(request.getDataType(), entity.getDataType())
                && equalsTextIfPresent(request.getApplicableObjectType(), entity.getApplicableObjectType())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && matchesCategoryKeyword(request.getKeyword(), entity);
    }

    private boolean matches(ConstraintQueryRequest request, TimeseriesConstraint entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getProjectId(), entity.getProjectId())
                && equalsIfPresent(request.getConstraintId(), entity.getConstraintId())
                && containsIfPresent(request.getConstraintName(), entity.getConstraintName())
                && equalsTextIfPresent(request.getEffectiveStatus(), entity.getEffectiveStatus())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && matchesConstraintKeyword(request.getKeyword(), entity);
    }

    private boolean matches(RelationQueryRequest request, TimeseriesRelation entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getProjectId(), entity.getProjectId())
                && equalsIfPresent(request.getRelationId(), entity.getRelationId())
                && containsIfPresent(request.getRelationName(), entity.getRelationName())
                && sourceContains(request.getSourceSequenceId(), entity)
                && equalsIfPresent(request.getTargetSequenceId(), entity.getTargetSequenceId())
                && equalsTextIfPresent(request.getRelationType(), entity.getRelationType())
                && equalsTextIfPresent(request.getEffectiveStatus(), entity.getEffectiveStatus())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && matchesRelationKeyword(request.getKeyword(), entity);
    }

    private boolean sourceContains(String sourceSequenceId, TimeseriesRelation entity) {
        return sourceSequenceId == null
                || (entity.getSourceSequences() != null && entity.getSourceSequences().contains(sourceSequenceId));
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

    private boolean matchesCategoryKeyword(String keyword, TimeseriesCategory entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getCategoryName())
                || containsIfPresent(keyword, entity.getCategoryDescription());
    }

    private boolean matchesConstraintKeyword(String keyword, TimeseriesConstraint entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getConstraintName())
                || containsIfPresent(keyword, entity.getConstraintDescription())
                || containsIfPresent(keyword, entity.getConstraintExpression());
    }

    private boolean matchesRelationKeyword(String keyword, TimeseriesRelation entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getRelationName())
                || containsIfPresent(keyword, entity.getTargetCategoryName())
                || containsIfPresent(keyword, entity.getRelationType())
                || containsIfPresent(keyword, entity.getLagRange());
    }

    private boolean isValidSequenceOrCategory(String projectId, String id) {
        if (id == null || id.isBlank()) return false;
        return memoryCache.getInstanceBySequenceId(projectId, id) != null
                || memoryCache.getCategory(projectId, id).isPresent();
    }

    private void reSyncTasksForConstraint(String projectId, String constraintId) {
        reSyncTasksForConstraint(projectId, constraintId, null);
    }

    private void reSyncTasksForConstraint(String projectId, String constraintId,
            TimeseriesConstraint previousConstraint) {
        if (constraintId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        for (TimeseriesAnomalyTask task : memoryCache.listAnomalyTasks(projectId)) {
            if (Objects.equals(projectId, task.getProjectId())
                    && contextResolver.isConstraintReferencedByAnomalyTask(task, constraintId, previousConstraint)) {
                anomalyGrpcClient.syncAnomalyTask(task);
            }
        }
        for (TimeseriesForecastTask task : memoryCache.listForecastTasks(projectId)) {
            if (Objects.equals(projectId, task.getProjectId())
                    && contextResolver.isConstraintReferencedByForecastTask(task, constraintId, previousConstraint)) {
                forecastGrpcClient.syncForecastTask(task);
            }
        }
    }

    private void reSyncTasksForRelation(String projectId, String relationId) {
        if (relationId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        cacheManager.ensureTableLoaded(CachedTable.FORECAST_TASK);
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        TimeseriesRelation rel = memoryCache.getRelation(projectId, relationId).orElse(null);
        if (rel == null) return;
        Set<String> affectedSeqIds = resolveAffectedSequenceIds(projectId, rel);
        for (TimeseriesAnomalyTask task : memoryCache.listAnomalyTasks(projectId)) {
            if (Objects.equals(projectId, task.getProjectId())
                    && task.getSequenceIds() != null
                    && !java.util.Collections.disjoint(task.getSequenceIds(), affectedSeqIds)) {
                anomalyGrpcClient.syncAnomalyTask(task);
            }
        }
        for (TimeseriesForecastTask task : memoryCache.listForecastTasks(projectId)) {
            if (Objects.equals(projectId, task.getProjectId())
                    && task.getForecastObjects() != null
                    && !java.util.Collections.disjoint(task.getForecastObjects(), affectedSeqIds)) {
                forecastGrpcClient.syncForecastTask(task);
            }
        }
    }

    /**
     * Resolve all sequence IDs affected by a relation (sources + targets, categories expanded).
     */
    private Set<String> resolveAffectedSequenceIds(String projectId, TimeseriesRelation rel) {
        Set<String> ids = new HashSet<>();
        if (rel.getSourceSequences() != null) {
            for (String src : rel.getSourceSequences()) {
                if (memoryCache.getCategory(projectId, src).isPresent()) {
                    for (TimeseriesInstanceConfig inst : memoryCache.listInstanceConfigs(projectId)) {
                        if (Objects.equals(projectId, inst.getProjectId())
                                && src.equals(inst.getCategoryId()) && inst.getSequenceId() != null) {
                            ids.add(inst.getSequenceId());
                        }
                    }
                } else {
                    ids.add(src);
                }
            }
        }
        String tgt = rel.getTargetSequenceId();
        if (tgt != null) {
            if (memoryCache.getCategory(projectId, tgt).isPresent()) {
                for (TimeseriesInstanceConfig inst : memoryCache.listInstanceConfigs(projectId)) {
                    if (Objects.equals(projectId, inst.getProjectId())
                            && tgt.equals(inst.getCategoryId()) && inst.getSequenceId() != null) {
                        ids.add(inst.getSequenceId());
                    }
                }
            } else {
                ids.add(tgt);
            }
        }
        return ids;
    }
}

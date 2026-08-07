package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.CategoryQueryRequest;
import com.sfkg.timeseries.dto.CategorySaveRequest;
import com.sfkg.timeseries.dto.CategoryStatusUpdateRequest;
import com.sfkg.timeseries.dto.ConstraintQueryRequest;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
import com.sfkg.timeseries.dto.RelationQueryRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.mapper.TimeseriesCategoryMapper;
import com.sfkg.timeseries.mapper.TimeseriesConstraintMapper;
import com.sfkg.timeseries.mapper.TimeseriesRelationMapper;
import com.sfkg.timeseries.service.TimeseriesSemanticService;
import com.sfkg.timeseries.vo.CategoryVO;
import com.sfkg.timeseries.vo.ConstraintVO;
import com.sfkg.timeseries.vo.RelationVO;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesSemanticServiceImpl implements TimeseriesSemanticService {

    private final TimeseriesCategoryMapper categoryMapper;
    private final TimeseriesConstraintMapper constraintMapper;
    private final TimeseriesRelationMapper relationMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesCoreGrpcClient coreGrpcClient;

    public TimeseriesSemanticServiceImpl(
            TimeseriesCategoryMapper categoryMapper,
            TimeseriesConstraintMapper constraintMapper,
            TimeseriesRelationMapper relationMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesCoreGrpcClient coreGrpcClient) {
        this.categoryMapper = categoryMapper;
        this.constraintMapper = constraintMapper;
        this.relationMapper = relationMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.coreGrpcClient = coreGrpcClient;
    }

    @Override
    public List<CategoryVO> listCategories(CategoryQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        return memoryCache.listCategories().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toCategoryVO)
                .collect(Collectors.toList());
    }

    @Override
    public String saveCategory(CategorySaveRequest request) {
        return doSaveCategory(request, true);
    }

    public String createCategory(CategorySaveRequest request) {
        String categoryId = request != null ? request.getCategoryId() : null;
        if (categoryId != null) {
            cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
            if (memoryCache.getCategory(categoryId).isPresent()) {
                throw new BusinessException("category already exists: " + categoryId);
            }
        }
        return doSaveCategory(request, false);
    }

    private String doSaveCategory(CategorySaveRequest request, boolean isUpdate) {
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        String categoryId = request == null || request.getCategoryId() == null
                ? generateId()
                : request.getCategoryId();

        TimeseriesCategory entity = memoryCache.getCategory(categoryId)
                .orElseGet(TimeseriesCategory::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
        }
        entity.setCategoryId(categoryId);

        categoryMapper.insert(entity);
        memoryCache.putCategory(entity);
        syncSemanticToCore(categoryId);
        return categoryId;
    }

    @Override
    public void updateCategoryStatus(CategoryStatusUpdateRequest request) {
        if (request == null || request.getCategoryId() == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        TimeseriesCategory entity = memoryCache.getCategory(request.getCategoryId())
                .orElseGet(TimeseriesCategory::new);
        entity.setCategoryId(request.getCategoryId());
        if (request.getConfirmStatus() != null) {
            entity.setConfirmStatus(request.getConfirmStatus());
        }
        categoryMapper.updateById(entity);
        memoryCache.putCategory(entity);
    }

    @Override
    public List<ConstraintVO> listConstraints(ConstraintQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        return memoryCache.listConstraints().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toConstraintVO)
                .collect(Collectors.toList());
    }

    @Override
    public String saveConstraint(ConstraintSaveRequest request) {
        return doSaveConstraint(request, true);
    }

    public String createConstraint(ConstraintSaveRequest request) {
        String constraintId = request != null ? request.getConstraintId() : null;
        if (constraintId != null) {
            cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
            if (memoryCache.getConstraint(constraintId).isPresent()) {
                throw new BusinessException("constraint already exists: " + constraintId);
            }
        }
        return doSaveConstraint(request, false);
    }

    private String doSaveConstraint(ConstraintSaveRequest request, boolean isUpdate) {
        if (request != null) {
            validateConstraintExpression(request.getConstraintExpression());
            validateVariableMapping(request.getVariableMapping());
        }
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        String constraintId = request == null || request.getConstraintId() == null
                ? generateId()
                : request.getConstraintId();

        TimeseriesConstraint entity = memoryCache.getConstraint(constraintId)
                .orElseGet(TimeseriesConstraint::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
            // BeanUtils 无法转换 List<ConstraintTermDTO> → List<ConstraintTermItem>，需手动拷贝
            if (request.getTerms() != null) {
                entity.setTerms(request.getTerms().stream()
                        .map(dto -> {
                            TimeseriesConstraint.ConstraintTermItem item = new TimeseriesConstraint.ConstraintTermItem();
                            item.setVariable(dto.getVariable());
                            item.setCoefficient(dto.getCoefficient());
                            item.setSampleOffset(dto.getSampleOffset());
                            return item;
                        })
                        .collect(Collectors.toList()));
            }
        }
        entity.setConstraintId(constraintId);

        constraintMapper.insert(entity);
        memoryCache.putConstraint(entity);
        syncSemanticToCore(constraintId);
        return constraintId;
    }

    @Override
    public void updateConstraintStatus(ConstraintStatusUpdateRequest request) {
        if (request == null || request.getConstraintId() == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        TimeseriesConstraint entity = memoryCache.getConstraint(request.getConstraintId())
                .orElseGet(TimeseriesConstraint::new);
        entity.setConstraintId(request.getConstraintId());
        if (request.getConfirmStatus() != null) {
            entity.setConfirmStatus(request.getConfirmStatus());
        }
        if (request.getEffectiveStatus() != null) {
            entity.setEffectiveStatus(request.getEffectiveStatus());
        }
        constraintMapper.updateById(entity);
        memoryCache.putConstraint(entity);
        syncSemanticToCore(entity.getConstraintId());
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
        return memoryCache.listRelations().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toRelationVO)
                .collect(Collectors.toList());
    }

    @Override
    public String saveRelation(RelationSaveRequest request) {
        return doSaveRelation(request, true);
    }

    public String createRelation(RelationSaveRequest request) {
        String relationId = request != null ? request.getRelationId() : null;
        if (relationId != null) {
            cacheManager.ensureTableLoaded(CachedTable.RELATION);
            if (memoryCache.getRelation(relationId).isPresent()) {
                throw new BusinessException("relation already exists: " + relationId);
            }
        }
        return doSaveRelation(request, false);
    }

    private String doSaveRelation(RelationSaveRequest request, boolean isUpdate) {
        validateRelationConfig(request);
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        String relationId = request == null || request.getRelationId() == null
                ? generateId()
                : request.getRelationId();

        TimeseriesRelation entity = memoryCache.getRelation(relationId)
                .orElseGet(TimeseriesRelation::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
        }
        entity.setRelationId(relationId);
        entity.setTargetCategoryName(resolveCategoryName(entity.getTargetSequenceId()));

        relationMapper.insert(entity);
        memoryCache.putRelation(entity);
        syncSemanticToCore(relationId);
        return relationId;
    }

    @Override
    public void updateRelationStatus(RelationStatusUpdateRequest request) {
        if (request == null || request.getRelationId() == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        TimeseriesRelation entity = memoryCache.getRelation(request.getRelationId())
                .orElseGet(TimeseriesRelation::new);
        entity.setRelationId(request.getRelationId());
        if (request.getConfirmStatus() != null) {
            entity.setConfirmStatus(request.getConfirmStatus());
        }
        if (request.getEffectiveStatus() != null) {
            entity.setEffectiveStatus(request.getEffectiveStatus());
        }
        relationMapper.updateById(entity);
        memoryCache.putRelation(entity);
        syncSemanticToCore(entity.getRelationId());
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

    private static final Set<String> VALID_RELATION_TYPES = Set.of("CAUSE", "CAUSAL", "CORRELATION", "ASSOCIATION");

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
        if (variableMapping == null || variableMapping.isEmpty()) {
            throw new BusinessException("variable mapping must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        for (Map.Entry<String, String> entry : variableMapping.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw new BusinessException("variable name must not be empty in mapping");
            }
            if (entry.getValue() == null || entry.getValue().isBlank()) {
                throw new BusinessException("sequenceId must not be empty for variable: " + entry.getKey());
            }
            TimeseriesInstanceConfig instance = memoryCache.getInstanceBySequenceId(entry.getValue());
            if (instance == null) {
                throw new BusinessException("mapped sequence not found: " + entry.getValue()
                        + " for variable: " + entry.getKey());
            }
        }
    }

    @Override
    public void validateRelationConfig(RelationSaveRequest request) {
        if (request == null) {
            throw new BusinessException("relation config must not be null");
        }
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
            if (src == null || src.isBlank()) continue;
            if (src.equals(request.getTargetSequenceId())) {
                throw new BusinessException("source sequence cannot equal target: " + src);
            }
            sourceSet.add(src);
            if (!isValidSequenceOrCategory(src)) {
                throw new BusinessException("source sequence or category not found: " + src);
            }
        }
        if (sourceSet.isEmpty()) {
            throw new BusinessException("sourceSequences must contain valid sequence or category IDs");
        }
        if (!isValidSequenceOrCategory(request.getTargetSequenceId())) {
            throw new BusinessException("target sequence or category not found: " + request.getTargetSequenceId());
        }
        if (request.getRelationType() != null && !request.getRelationType().isBlank()
                && !VALID_RELATION_TYPES.contains(request.getRelationType().toUpperCase())) {
            throw new BusinessException("unsupported relationType: " + request.getRelationType()
                    + ". Supported: " + VALID_RELATION_TYPES);
        }
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

    @Override
    public void syncSemanticToGraph(String semanticId) {
        // TODO: Restore graph synchronization here.
    }

    @Override
    public void syncSemanticToCore(String semanticId) {
        if (semanticId == null) return;
        cacheManager.ensureTableLoaded(CachedTable.CONSTRAINT);
        cacheManager.ensureTableLoaded(CachedTable.RELATION);
        memoryCache.getConstraint(semanticId).ifPresent(coreGrpcClient::syncConstraintConfig);
        memoryCache.getRelation(semanticId).ifPresent(coreGrpcClient::syncRelationConfig);
    }

    private String generateId() {
        return UUID.randomUUID().toString();
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

    private String resolveCategoryName(String categoryId) {
        if (categoryId == null) {
            return null;
        }
        cacheManager.ensureTableLoaded(CachedTable.CATEGORY);
        return memoryCache.getCategory(categoryId)
                .map(TimeseriesCategory::getCategoryName)
                .orElse(null);
    }

    private boolean matches(CategoryQueryRequest request, TimeseriesCategory entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getCategoryId(), entity.getCategoryId())
                && containsIfPresent(request.getCategoryName(), entity.getCategoryName())
                && equalsTextIfPresent(request.getDataType(), entity.getDataType())
                && equalsTextIfPresent(request.getApplicableObjectType(), entity.getApplicableObjectType())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus());
    }

    private boolean matches(ConstraintQueryRequest request, TimeseriesConstraint entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getConstraintId(), entity.getConstraintId())
                && containsIfPresent(request.getConstraintName(), entity.getConstraintName())
                && equalsIfPresent(request.getCategoryId(), entity.getCategoryId())
                && equalsTextIfPresent(request.getEffectiveStatus(), entity.getEffectiveStatus())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && matchesConstraintKeyword(request.getKeyword(), entity);
    }

    private boolean matches(RelationQueryRequest request, TimeseriesRelation entity) {
        if (request == null) {
            return true;
        }
        return equalsIfPresent(request.getRelationId(), entity.getRelationId())
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

    private boolean isValidSequenceOrCategory(String id) {
        if (id == null || id.isBlank()) return false;
        return memoryCache.getInstanceBySequenceId(id) != null
                || memoryCache.getCategory(id).isPresent();
    }
}

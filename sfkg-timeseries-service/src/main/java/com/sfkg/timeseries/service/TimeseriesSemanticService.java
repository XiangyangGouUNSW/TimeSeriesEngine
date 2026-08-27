package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.CategoryQueryRequest;
import com.sfkg.timeseries.dto.CategorySaveRequest;
import com.sfkg.timeseries.dto.CategoryStatusUpdateRequest;
import com.sfkg.timeseries.dto.ConstraintQueryRequest;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
import com.sfkg.timeseries.dto.RelationQueryRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.vo.CategoryVO;
import com.sfkg.timeseries.vo.ConstraintVO;
import com.sfkg.timeseries.vo.RelationVO;
import java.util.List;
import java.util.Map;

public interface TimeseriesSemanticService {

    List<CategoryVO> listCategories(CategoryQueryRequest request);

    String saveCategory(CategorySaveRequest request);

    String createCategory(CategorySaveRequest request);

    void updateCategoryStatus(CategoryStatusUpdateRequest request);

    List<ConstraintVO> listConstraints(ConstraintQueryRequest request);

    String saveConstraint(ConstraintSaveRequest request);

    String createConstraint(ConstraintSaveRequest request);

    /**
     * OR 组批量创建：整体校验、整体落库后一次性同步 Core，返回全部约束 ID。
     */
    List<String> createConstraintBatch(com.sfkg.timeseries.dto.ConstraintBatchSaveRequest request);

    void updateConstraintStatus(ConstraintStatusUpdateRequest request);

    void updateConstraintStatus(String constraintId, String status);

    List<RelationVO> listRelations(RelationQueryRequest request);

    String saveRelation(RelationSaveRequest request);

    String createRelation(RelationSaveRequest request);

    void updateRelationStatus(RelationStatusUpdateRequest request);

    void updateRelationStatus(String relationId, String status);

    void validateConstraintExpression(String expression);

    void validateVariableMapping(Map<String, String> variableMapping);

    void validateRelationConfig(RelationSaveRequest request);

    void syncSemanticToGraph(String semanticId);

    void syncSemanticToCore(String semanticId);
}

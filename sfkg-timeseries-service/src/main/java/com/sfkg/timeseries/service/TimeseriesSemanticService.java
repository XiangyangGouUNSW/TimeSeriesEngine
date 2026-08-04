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

    Integer saveCategory(CategorySaveRequest request);

    void updateCategoryStatus(CategoryStatusUpdateRequest request);

    List<ConstraintVO> listConstraints(ConstraintQueryRequest request);

    Integer saveConstraint(ConstraintSaveRequest request);

    void updateConstraintStatus(ConstraintStatusUpdateRequest request);

    void updateConstraintStatus(Integer constraintId, String status);

    List<RelationVO> listRelations(RelationQueryRequest request);

    Integer saveRelation(RelationSaveRequest request);

    void updateRelationStatus(RelationStatusUpdateRequest request);

    void updateRelationStatus(Integer relationId, String status);

    void validateConstraintExpression(String expression);

    void validateVariableMapping(Map<String, Integer> variableMapping);

    void validateRelationConfig(RelationSaveRequest request);

    void syncSemanticToGraph(Integer semanticId);

    void syncSemanticToCore(Integer semanticId);
}

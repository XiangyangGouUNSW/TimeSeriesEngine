package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
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
import com.sfkg.timeseries.service.TimeseriesSemanticService;
import com.sfkg.timeseries.vo.CategoryVO;
import com.sfkg.timeseries.vo.ConstraintVO;
import com.sfkg.timeseries.vo.RelationVO;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/semantic")
public class TimeseriesSemanticController {

    private final TimeseriesSemanticService semanticService;

    public TimeseriesSemanticController(TimeseriesSemanticService semanticService) {
        this.semanticService = semanticService;
    }

    @GetMapping(value = "/categories", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<CategoryVO>> listCategories() {
        List<CategoryVO> data = semanticService.listCategories(null);
        return returnSuccess("semantic category query success", data);
    }

    @PostMapping(value = "/categories/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<CategoryVO>> listCategoriesByJson(@RequestBody CategoryQueryRequest request) {
        List<CategoryVO> data = semanticService.listCategories(request);
        return returnSuccess("semantic category query success", data);
    }

    @PostMapping(value = "/categories", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveCategory(@RequestBody CategorySaveRequest request) {
        semanticService.createCategory(request);
        return returnSuccess("semantic category save success");
    }

    @PutMapping(value = "/categories", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateCategory(@RequestBody CategorySaveRequest request) {
        semanticService.saveCategory(request);
        return returnSuccess("semantic category update success");
    }

    @PatchMapping(value = "/categories/status", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateCategoryStatus(@RequestBody CategoryStatusUpdateRequest request) {
        semanticService.updateCategoryStatus(request);
        return returnSuccess("semantic category status update success");
    }

    @GetMapping(value = "/constraints", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<ConstraintVO>> listConstraints() {
        List<ConstraintVO> data = semanticService.listConstraints(null);
        return returnSuccess("semantic constraint query success", data);
    }

    @PostMapping(value = "/constraints/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<ConstraintVO>> listConstraintsByJson(@RequestBody ConstraintQueryRequest request) {
        List<ConstraintVO> data = semanticService.listConstraints(request);
        return returnSuccess("semantic constraint query success", data);
    }

    @PostMapping(value = "/constraints", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveConstraint(@RequestBody ConstraintSaveRequest request) {
        semanticService.createConstraint(request);
        return returnSuccess("semantic constraint save success");
    }

    @PostMapping(value = "/constraints/batch", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<String>> saveConstraintBatch(@RequestBody ConstraintBatchSaveRequest request) {
        List<String> ids = semanticService.createConstraintBatch(request);
        return returnSuccess("semantic constraint batch save success", ids);
    }

    @PutMapping(value = "/constraints", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateConstraint(@RequestBody ConstraintSaveRequest request) {
        semanticService.saveConstraint(request);
        return returnSuccess("semantic constraint update success");
    }

    @PatchMapping(value = "/constraints/status", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateConstraintStatus(@RequestBody ConstraintStatusUpdateRequest request) {
        semanticService.updateConstraintStatus(request);
        return returnSuccess("semantic constraint status update success");
    }

    @GetMapping(value = "/relations", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<RelationVO>> listRelations() {
        List<RelationVO> data = semanticService.listRelations(null);
        return returnSuccess("semantic relation query success", data);
    }

    @PostMapping(value = "/relations/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<RelationVO>> listRelationsByJson(@RequestBody RelationQueryRequest request) {
        List<RelationVO> data = semanticService.listRelations(request);
        return returnSuccess("semantic relation query success", data);
    }

    @PostMapping(value = "/relations", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveRelation(@RequestBody RelationSaveRequest request) {
        semanticService.createRelation(request);
        return returnSuccess("semantic relation save success");
    }

    @PutMapping(value = "/relations", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateRelation(@RequestBody RelationSaveRequest request) {
        semanticService.saveRelation(request);
        return returnSuccess("semantic relation update success");
    }

    @PatchMapping(value = "/relations/status", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateRelationStatus(@RequestBody RelationStatusUpdateRequest request) {
        semanticService.updateRelationStatus(request);
        return returnSuccess("semantic relation status update success");
    }
}

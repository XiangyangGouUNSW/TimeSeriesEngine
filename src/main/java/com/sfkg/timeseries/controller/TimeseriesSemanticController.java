package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.CategoryQueryRequest;
import com.sfkg.timeseries.dto.CategorySaveRequest;
import com.sfkg.timeseries.dto.CategoryStatusUpdateRequest;
import com.sfkg.timeseries.dto.ConstraintQueryRequest;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
import com.sfkg.timeseries.dto.RelationQueryRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.service.TimeseriesSemanticService;
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
    public ApiResult<Void> listCategories() {
        semanticService.listCategories(null);
        return returnSuccess("semantic category query success");
    }

    @PostMapping(value = "/categories/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> listCategoriesByJson(@RequestBody CategoryQueryRequest request) {
        semanticService.listCategories(request);
        return returnSuccess("semantic category query success");
    }

    @PostMapping(value = "/categories", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveCategory(@RequestBody CategorySaveRequest request) {
        semanticService.saveCategory(request);
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
    public ApiResult<Void> listConstraints() {
        semanticService.listConstraints(null);
        return returnSuccess("semantic constraint query success");
    }

    @PostMapping(value = "/constraints/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> listConstraintsByJson(@RequestBody ConstraintQueryRequest request) {
        semanticService.listConstraints(request);
        return returnSuccess("semantic constraint query success");
    }

    @PostMapping(value = "/constraints", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveConstraint(@RequestBody ConstraintSaveRequest request) {
        semanticService.saveConstraint(request);
        return returnSuccess("semantic constraint save success");
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
    public ApiResult<Void> listRelations() {
        semanticService.listRelations(null);
        return returnSuccess("semantic relation query success");
    }

    @PostMapping(value = "/relations/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> listRelationsByJson(@RequestBody RelationQueryRequest request) {
        semanticService.listRelations(request);
        return returnSuccess("semantic relation query success");
    }

    @PostMapping(value = "/relations", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveRelation(@RequestBody RelationSaveRequest request) {
        semanticService.saveRelation(request);
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

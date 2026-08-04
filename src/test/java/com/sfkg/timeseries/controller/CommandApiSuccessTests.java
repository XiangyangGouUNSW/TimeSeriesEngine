package com.sfkg.timeseries.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.dto.AnomalyTaskSaveRequest;
import com.sfkg.timeseries.dto.CategoryQueryRequest;
import com.sfkg.timeseries.dto.CategorySaveRequest;
import com.sfkg.timeseries.dto.CategoryStatusUpdateRequest;
import com.sfkg.timeseries.dto.ConstraintQueryRequest;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
// import com.sfkg.timeseries.dto.DecisionQueryRequest;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
// import com.sfkg.timeseries.dto.EventDetailQueryRequest;
import com.sfkg.timeseries.dto.EventQueryRequest;
import com.sfkg.timeseries.dto.EventSaveRequest;
import com.sfkg.timeseries.dto.ForecastResultQueryRequest;
import com.sfkg.timeseries.dto.ForecastTaskSaveRequest;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigSaveRequest;
import com.sfkg.timeseries.dto.RelationQueryRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.service.TimeseriesAnomalyResultService;
import com.sfkg.timeseries.service.TimeseriesAnomalyTaskService;
import com.sfkg.timeseries.service.TimeseriesDataService;
import com.sfkg.timeseries.service.TimeseriesDecisionService;
import com.sfkg.timeseries.service.TimeseriesEventService;
import com.sfkg.timeseries.service.TimeseriesForecastResultService;
import com.sfkg.timeseries.service.TimeseriesForecastTaskService;
import com.sfkg.timeseries.service.TimeseriesInstanceService;
import com.sfkg.timeseries.service.TimeseriesSemanticService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class CommandApiSuccessTests {

    private static final String INSTANCE_SAVE_JSON = """
            {
              "sequenceId": 1001,
              "instanceName": "main transformer oil temperature",
              "externalSequenceId": 9001,
              "categoryId": 2001,
              "deviceInstanceId": 3001,
              "dataSourceId": 4001,
              "accessStatus": "ENABLE"
            }
            """;

    private static final String INSTANCE_QUERY_JSON = """
            {
              "sequenceId": 1001,
              "categoryId": 2001,
              "deviceInstanceId": 3001,
              "accessStatus": "ENABLE"
            }
            """;

    private static final String HISTORY_QUERY_JSON = """
            {
              "sequenceId": 1001,
              "startTime": "2026-08-03T10:00:00",
              "endTime": "2026-08-03T11:00:00",
              "granularity": "1m"
            }
            """;

    private static final String TIMESERIES_DATA_SAVE_JSON = """
            {
              "sequence_id": 1001,
              "data": {
                "2026-08-03T10:00:00": 32.5,
                "2026-08-03T10:01:00": 33.1,
                "2026-08-03T10:02:00": 32.8
              }
            }
            """;

    private static final String CATEGORY_SAVE_JSON = """
            {
              "categoryId": 2001,
              "categoryName": "temperature",
              "dataType": "DOUBLE",
              "categoryDescription": "temperature semantic category",
              "applicableObjectType": "transformer",
              "defaultUnit": "celsius",
              "confirmStatus": "CONFIRMED"
            }
            """;

    private static final String CATEGORY_QUERY_JSON = """
            {
              "categoryId": 2001,
              "categoryName": "temperature",
              "dataType": "DOUBLE",
              "applicableObjectType": "transformer",
              "confirmStatus": "CONFIRMED"
            }
            """;

    private static final String CONSTRAINT_SAVE_JSON = """
            {
              "constraintId": 5001,
              "constraintName": "temperature upper limit",
              "categoryId": 2001,
              "variableMapping": {"x": 1001},
              "constraintDescription": "temperature should be lower than threshold",
              "constraintExpression": "x < 100",
              "lag": "0m",
              "effectiveStatus": "ENABLE",
              "confirmStatus": "CONFIRMED"
            }
            """;

    private static final String CONSTRAINT_QUERY_JSON = """
            {
              "constraintId": 5001,
              "constraintName": "temperature upper limit",
              "categoryId": 2001,
              "effectiveStatus": "ENABLE",
              "confirmStatus": "CONFIRMED",
              "keyword": "temperature"
            }
            """;

    private static final String RELATION_SAVE_JSON = """
            {
              "relationId": 6001,
              "relationName": "load affects temperature",
              "sourceCategories": [2002],
              "targetCategoryId": 2001,
              "relationType": "CAUSE",
              "lagRange": "0m-10m",
              "confidence": 0.86,
              "relationDescription": "load changes may affect transformer oil temperature with a short lag",
              "effectiveStatus": "ENABLE",
              "confirmStatus": "CONFIRMED"
            }
            """;

    private static final String RELATION_QUERY_JSON = """
            {
              "relationId": 6001,
              "relationName": "load affects temperature",
              "sourceCategoryId": 2002,
              "targetCategoryId": 2001,
              "relationType": "CAUSE",
              "effectiveStatus": "ENABLE",
              "confirmStatus": "CONFIRMED",
              "keyword": "load"
            }
            """;

    private static final String CATEGORY_STATUS_JSON = """
            {
              "categoryId": 2001,
              "confirmStatus": "CONFIRMED",
              "effectiveStatus": "ENABLE"
            }
            """;

    private static final String CONSTRAINT_STATUS_JSON = """
            {
              "constraintId": 5001,
              "confirmStatus": "CONFIRMED",
              "effectiveStatus": "ENABLE"
            }
            """;

    private static final String RELATION_STATUS_JSON = """
            {
              "relationId": 6001,
              "confirmStatus": "CONFIRMED",
              "effectiveStatus": "ENABLE"
            }
            """;

    private static final String ANOMALY_TASK_SAVE_JSON = """
            {
              "taskId": 7001,
              "taskName": "temperature anomaly detection",
              "detectObjects": [1001, 1002],
              "detectMethod": "RULE",
              "warningRule": "temperature > 90",
              "status": "ENABLE"
            }
            """;

    private static final String FORECAST_TASK_SAVE_JSON = """
            {
              "taskId": 7002,
              "taskName": "temperature forecast",
              "forecastObjects": [1001, 1002],
              "forecastHorizon": "24h",
              "warningRule": "forecast temperature > 90",
              "status": "ENABLE"
            }
            """;

    private static final String ANOMALY_TASK_QUERY_JSON = """
            {
              "taskId": 7001,
              "taskName": "temperature",
              "taskType": "ANOMALY",
              "status": "ENABLE",
              "keyword": "temperature"
            }
            """;

    private static final String ANOMALY_TASK_STATUS_JSON = """
            {
              "taskId": 7001,
              "taskType": "ANOMALY",
              "status": "DISABLE"
            }
            """;

    private static final String FORECAST_TASK_QUERY_JSON = """
            {
              "taskId": 7002,
              "taskName": "temperature",
              "taskType": "FORECAST",
              "status": "ENABLE",
              "keyword": "temperature"
            }
            """;

    private static final String FORECAST_TASK_STATUS_JSON = """
            {
              "taskId": 7002,
              "taskType": "FORECAST",
              "status": "DISABLE"
            }
            """;

    private static final String EVENT_QUERY_JSON = """
            {
              "eventType": "ANOMALY",
              "eventSource": "ANOMALY_DETECTION",
              "eventLevel": "WARNING",
              "confirmStatus": "CONFIRMED",
              "handleStatus": "UNHANDLED",
              "relatedSequences": [1001],
              "startTime": "2026-08-03T10:00:00",
              "endTime": "2026-08-03T11:00:00"
            }
            """;

    private static final String EVENT_DETAIL_JSON = """
            {
              "eventId": 8001
            }
            """;

    private static final String EVENT_SAVE_JSON = """
            {
              "eventId": 8001,
              "eventName": "temperature warning",
              "eventType": "ANOMALY",
              "eventSource": "ANOMALY_DETECTION",
              "relatedSequences": [1001, 1002],
              "relatedRules": [5001],
              "eventDescription": "oil temperature is higher than the configured warning threshold",
              "eventLevel": "WARNING",
              "eventTime": "2026-08-03T10:30:00",
              "confirmStatus": "CONFIRMED",
              "handleStatus": "UNHANDLED",
              "diagnosisResult": "temperature increase may be related to load fluctuation",
              "diagnosisBasis": "related sequence 1001 triggered constraint 5001",
              "disposalResult": "pending manual disposal"
            }
            """;

    private static final String DECISION_QUERY_JSON = """
            {
              "eventId": 8001
            }
            """;

    private static final String FEEDBACK_JSON = """
            {
              "eventId": 8001,
              "disposalResult": "confirmed and processed",
              "handleStatus": "HANDLED"
            }
            """;

    private static final String ANOMALY_RESULT_QUERY_JSON = """
            {
              "taskId": 7001,
              "sequenceId": 1001,
              "startTime": "2026-08-03T10:00:00",
              "endTime": "2026-08-03T11:00:00",
              "eventLevel": "WARNING"
            }
            """;

    private static final String FORECAST_RESULT_QUERY_JSON = """
            {
              "taskId": 7002,
              "sequenceId": 1001,
              "startTime": "2026-08-03T10:00:00",
              "endTime": "2026-08-03T11:00:00"
            }
            """;

    private final TimeseriesInstanceService instanceService = mock(TimeseriesInstanceService.class);
    private final TimeseriesDataService dataService = mock(TimeseriesDataService.class);
    private final TimeseriesSemanticService semanticService = mock(TimeseriesSemanticService.class);
    private final TimeseriesAnomalyTaskService anomalyTaskService = mock(TimeseriesAnomalyTaskService.class);
    private final TimeseriesForecastTaskService forecastTaskService = mock(TimeseriesForecastTaskService.class);
    private final TimeseriesEventService eventService = mock(TimeseriesEventService.class);
    private final TimeseriesDecisionService decisionService = mock(TimeseriesDecisionService.class);
    private final TimeseriesAnomalyResultService anomalyResultService = mock(TimeseriesAnomalyResultService.class);
    private final TimeseriesForecastResultService forecastResultService = mock(TimeseriesForecastResultService.class);

    private final MockMvc mockMvc = MockMvcBuilders
            .standaloneSetup(
                    new TimeseriesInstanceController(instanceService),
                    new TimeseriesDataController(dataService),
                    new TimeseriesSemanticController(semanticService),
                    new TimeseriesAnomalyTaskController(anomalyTaskService),
                    new TimeseriesForecastTaskController(forecastTaskService),
                    new TimeseriesEventController(eventService),
                    new TimeseriesDecisionController(decisionService),
                    new TimeseriesAnomalyResultController(anomalyResultService),
                    new TimeseriesForecastResultController(forecastResultService))
            .build();

    @Test
    void commandJsonRequestsReturnSpecificSuccessMessages() throws Exception {
        List<CommandRequest> requests = List.of(
                new CommandRequest(HttpMethod.POST, "/api/timeseries/instances", INSTANCE_SAVE_JSON,
                        "timeseries instance create success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/instances", INSTANCE_SAVE_JSON,
                        "timeseries instance update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/instances/query", INSTANCE_QUERY_JSON,
                        "timeseries instance query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/data/history/query", HISTORY_QUERY_JSON,
                        "history data query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/data/points", TIMESERIES_DATA_SAVE_JSON,
                        "timeseries data save success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/semantic/categories/query", CATEGORY_QUERY_JSON,
                        "semantic category query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/semantic/categories", CATEGORY_SAVE_JSON,
                        "semantic category save success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/semantic/categories", CATEGORY_SAVE_JSON,
                        "semantic category update success"),
                new CommandRequest(HttpMethod.PATCH, "/api/timeseries/semantic/categories/status", CATEGORY_STATUS_JSON,
                        "semantic category status update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/semantic/constraints/query", CONSTRAINT_QUERY_JSON,
                        "semantic constraint query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/semantic/constraints", CONSTRAINT_SAVE_JSON,
                        "semantic constraint save success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/semantic/constraints", CONSTRAINT_SAVE_JSON,
                        "semantic constraint update success"),
                new CommandRequest(HttpMethod.PATCH, "/api/timeseries/semantic/constraints/status", CONSTRAINT_STATUS_JSON,
                        "semantic constraint status update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/semantic/relations/query", RELATION_QUERY_JSON,
                        "semantic relation query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/semantic/relations", RELATION_SAVE_JSON,
                        "semantic relation save success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/semantic/relations", RELATION_SAVE_JSON,
                        "semantic relation update success"),
                new CommandRequest(HttpMethod.PATCH, "/api/timeseries/semantic/relations/status", RELATION_STATUS_JSON,
                        "semantic relation status update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/anomaly-tasks", ANOMALY_TASK_SAVE_JSON,
                        "anomaly task create success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/anomaly-tasks", ANOMALY_TASK_SAVE_JSON,
                        "anomaly task update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/anomaly-tasks/query", ANOMALY_TASK_QUERY_JSON,
                        "anomaly task list query success"),
                new CommandRequest(HttpMethod.PATCH, "/api/timeseries/anomaly-tasks/status", ANOMALY_TASK_STATUS_JSON,
                        "anomaly task status update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/forecast-tasks", FORECAST_TASK_SAVE_JSON,
                        "forecast task create success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/forecast-tasks", FORECAST_TASK_SAVE_JSON,
                        "forecast task update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/forecast-tasks/query", FORECAST_TASK_QUERY_JSON,
                        "forecast task list query success"),
                new CommandRequest(HttpMethod.PATCH, "/api/timeseries/forecast-tasks/status", FORECAST_TASK_STATUS_JSON,
                        "forecast task status update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/events/query", EVENT_QUERY_JSON,
                        "event list query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/events/detail", EVENT_DETAIL_JSON,
                        "event detail query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/events", EVENT_SAVE_JSON,
                        "event save success"),
                new CommandRequest(HttpMethod.PUT, "/api/timeseries/events", EVENT_SAVE_JSON,
                        "event update success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/decision/diagnosis", DECISION_QUERY_JSON,
                        "diagnosis query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/decision/suggestion", DECISION_QUERY_JSON,
                        "decision suggestion query success"),
                new CommandRequest(HttpMethod.PATCH, "/api/timeseries/decision/feedback", FEEDBACK_JSON,
                        "disposal feedback submit success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/anomaly-results/query", ANOMALY_RESULT_QUERY_JSON,
                        "anomaly result query success"),
                new CommandRequest(HttpMethod.POST, "/api/timeseries/forecast-results/query", FORECAST_RESULT_QUERY_JSON,
                        "forecast result query success")
        );

        for (CommandRequest request : requests) {
            mockMvc.perform(MockMvcRequestBuilders.request(request.method(), request.path())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(request.body()))
                    .andExpect(status().isOk())
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                    .andExpect(jsonPath("$.success").value(true))
                    .andExpect(jsonPath("$.message").value(request.expectedSuccess()));
        }

        verify(instanceService, times(2)).saveInstanceConfig(org.mockito.ArgumentMatchers.any(InstanceConfigSaveRequest.class));
        verify(instanceService).queryInstanceConfigs(org.mockito.ArgumentMatchers.any(InstanceConfigQueryRequest.class));
        verify(dataService).queryHistoryData(org.mockito.ArgumentMatchers.any(HistoryDataQueryRequest.class));
        verify(dataService).saveTimeseriesData(org.mockito.ArgumentMatchers.any(TimeseriesDataSaveRequest.class));

        verify(semanticService).listCategories(org.mockito.ArgumentMatchers.any(CategoryQueryRequest.class));
        verify(semanticService, times(2)).saveCategory(org.mockito.ArgumentMatchers.any(CategorySaveRequest.class));
        verify(semanticService).updateCategoryStatus(org.mockito.ArgumentMatchers.any(CategoryStatusUpdateRequest.class));
        verify(semanticService).listConstraints(org.mockito.ArgumentMatchers.any(ConstraintQueryRequest.class));
        verify(semanticService, times(2)).saveConstraint(org.mockito.ArgumentMatchers.any(ConstraintSaveRequest.class));
        verify(semanticService).updateConstraintStatus(org.mockito.ArgumentMatchers.any(ConstraintStatusUpdateRequest.class));
        verify(semanticService).listRelations(org.mockito.ArgumentMatchers.any(RelationQueryRequest.class));
        verify(semanticService, times(2)).saveRelation(org.mockito.ArgumentMatchers.any(RelationSaveRequest.class));
        verify(semanticService).updateRelationStatus(org.mockito.ArgumentMatchers.any(RelationStatusUpdateRequest.class));

        verify(anomalyTaskService, times(2)).saveAnomalyTask(org.mockito.ArgumentMatchers.any(AnomalyTaskSaveRequest.class));
        verify(anomalyTaskService).listAnomalyTasks(org.mockito.ArgumentMatchers.any(TaskQueryRequest.class));
        verify(anomalyTaskService).updateAnomalyTaskStatus(org.mockito.ArgumentMatchers.any(TaskStatusUpdateRequest.class));
        verify(forecastTaskService, times(2)).saveForecastTask(org.mockito.ArgumentMatchers.any(ForecastTaskSaveRequest.class));
        verify(forecastTaskService).listForecastTasks(org.mockito.ArgumentMatchers.any(TaskQueryRequest.class));
        verify(forecastTaskService).updateForecastTaskStatus(org.mockito.ArgumentMatchers.any(TaskStatusUpdateRequest.class));

        verify(eventService).listEvents(org.mockito.ArgumentMatchers.any(EventQueryRequest.class));
        verify(eventService).getEventDetail(org.mockito.ArgumentMatchers.anyInt());
        verify(eventService, times(2)).saveEvent(org.mockito.ArgumentMatchers.any(EventSaveRequest.class));
        verify(decisionService).getDiagnosisResult(org.mockito.ArgumentMatchers.anyInt());
        verify(decisionService).getDecisionSuggestion(org.mockito.ArgumentMatchers.anyInt());
        verify(decisionService).submitDisposalFeedback(org.mockito.ArgumentMatchers.any(DisposalFeedbackRequest.class));
        verify(anomalyResultService).queryAnomalyResults(org.mockito.ArgumentMatchers.any(AnomalyResultQueryRequest.class));
        verify(forecastResultService).queryForecastResults(org.mockito.ArgumentMatchers.any(ForecastResultQueryRequest.class));
    }

    private record CommandRequest(
            HttpMethod method,
            String path,
            String body,
            String expectedSuccess) {
    }
}


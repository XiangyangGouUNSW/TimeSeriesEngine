package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.AnomalyTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.vo.AnomalyTaskVO;
import java.util.List;

public interface TimeseriesAnomalyTaskService {

    String saveAnomalyTask(AnomalyTaskSaveRequest request);

    String createAnomalyTask(AnomalyTaskSaveRequest request);

    List<AnomalyTaskVO> listAnomalyTasks();

    List<AnomalyTaskVO> listAnomalyTasks(TaskQueryRequest request);

    void updateAnomalyTaskStatus(TaskStatusUpdateRequest request);

    void validateDetectObjects(AnomalyTaskSaveRequest request);

    void validateDetectMethod(String detectMethod);

    void syncAnomalyTaskToCore(String taskId);

    void syncAnomalyTaskToAnomalyService(String taskId);
}

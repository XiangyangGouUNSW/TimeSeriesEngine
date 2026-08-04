package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.ForecastTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.vo.ForecastTaskVO;
import java.util.List;

public interface TimeseriesForecastTaskService {

    Integer saveForecastTask(ForecastTaskSaveRequest request);

    List<ForecastTaskVO> listForecastTasks();

    List<ForecastTaskVO> listForecastTasks(TaskQueryRequest request);

    void updateForecastTaskStatus(TaskStatusUpdateRequest request);

    void validateForecastObjects(ForecastTaskSaveRequest request);

    void validateForecastHorizon(String forecastHorizon);

    void syncForecastTaskToCore(Integer taskId);

    void syncForecastTaskToForecastService(Integer taskId);
}

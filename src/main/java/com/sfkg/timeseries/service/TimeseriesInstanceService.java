package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigSaveRequest;
import com.sfkg.timeseries.vo.InstanceConfigVO;
import java.util.List;

public interface TimeseriesInstanceService {

    Integer saveInstanceConfig(InstanceConfigSaveRequest request);

    List<InstanceConfigVO> queryInstanceConfigs(InstanceConfigQueryRequest request);

    void validateCategory(Integer categoryId);

    void validateDeviceInstance(Integer deviceInstanceId);

    Integer generateSequenceId();

    void syncInstanceToGraph(Integer sequenceId);

    void syncInstanceToCore(Integer sequenceId);
}

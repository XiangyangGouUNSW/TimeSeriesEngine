package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigSaveRequest;
import com.sfkg.timeseries.vo.InstanceConfigVO;
import java.util.List;

public interface TimeseriesInstanceService {

    String saveInstanceConfig(InstanceConfigSaveRequest request);

    List<InstanceConfigVO> queryInstanceConfigs(InstanceConfigQueryRequest request);

    void validateCategory(String categoryId);

    void validateDeviceInstance(String deviceInstanceId);

    String generateSequenceId();

    void syncInstanceToGraph(String sequenceId);

    void syncInstanceToCore(String sequenceId);
}

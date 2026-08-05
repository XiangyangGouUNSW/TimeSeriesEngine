package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.service.TimeseriesDataService;
import com.sfkg.timeseries.vo.HistoryDataVO;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/data")
public class TimeseriesDataController {

    private final TimeseriesDataService dataService;

    public TimeseriesDataController(TimeseriesDataService dataService) {
        this.dataService = dataService;
    }

    @GetMapping(value = "/history", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<HistoryDataVO> queryHistoryData() {
        HistoryDataVO data = dataService.queryHistoryData(null);
        return returnSuccess("history data query success", data);
    }

    @PostMapping(
            value = "/points",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveTimeseriesData(@RequestBody TimeseriesDataSaveRequest request) {
        dataService.saveTimeseriesData(request);
        return returnSuccess("timeseries data save success");
    }

    @PostMapping(
            value = "/ingest",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> ingestData(@RequestBody TimeseriesDataSaveRequest request) {
        dataService.saveTimeseriesData(request);
        return returnSuccess("timeseries data ingest success");
    }

    @PostMapping(
            value = "/history/query",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<HistoryDataVO> queryHistoryDataByJson(@RequestBody HistoryDataQueryRequest request) {
        HistoryDataVO data = dataService.queryHistoryData(request);
        return returnSuccess("history data query success", data);
    }

    @PostMapping(
            value = "/history/overview",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Map<String, Object>> queryHistoryOverview(@RequestBody HistoryDataQueryRequest request) {
        Map<String, Object> data = dataService.queryHistoryOverview(request);
        return returnSuccess("history overview query success", data);
    }

    @PostMapping(
            value = "/window/query",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Map<String, Object>> queryWindowData(@RequestBody HistoryDataQueryRequest request) {
        Map<String, Object> data = dataService.queryWindowData(request);
        return returnSuccess("window data query success", data);
    }
}

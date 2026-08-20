package com.sfkg.timeseries.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sfkg.timeseries.service.TimeseriesDataService;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class TimeseriesDataControllerTests {

    private final TimeseriesDataService dataService = mock(TimeseriesDataService.class);
    private final MockMvc mockMvc = MockMvcBuilders
            .standaloneSetup(new TimeseriesDataController(dataService))
            .build();

    @Test
    void queryHistoryDataReturnsSuccess() throws Exception {
        String json = """
                {
                  "projectId": "project-a",
                  "sequenceId": 1001,
                  "startTime": "2026-08-03T10:00:00",
                  "endTime": "2026-08-03T11:00:00",
                  "granularity": 60000
                }
                """;

        mockMvc.perform(post("/api/timeseries/data/history/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("history data query success"));
    }

    @Test
    void saveTimeseriesDataReturnsSuccess() throws Exception {
        String json = """
                {
                  "projectId": "project-a",
                  "sequence_id": 1001,
                  "data": {
                    "2026-08-03T10:00:00": 32.5,
                    "2026-08-03T10:01:00": 33.1
                  }
                }
                """;

        mockMvc.perform(post("/api/timeseries/data/points")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("timeseries data save success"));

        verify(dataService).saveTimeseriesData(org.mockito.ArgumentMatchers.any(TimeseriesDataSaveRequest.class));
    }
}

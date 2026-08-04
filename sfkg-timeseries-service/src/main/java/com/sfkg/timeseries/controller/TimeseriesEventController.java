package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.EventDetailQueryRequest;
import com.sfkg.timeseries.dto.EventQueryRequest;
import com.sfkg.timeseries.dto.EventSaveRequest;
import com.sfkg.timeseries.service.TimeseriesEventService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/events")
public class TimeseriesEventController {

    private final TimeseriesEventService eventService;

    public TimeseriesEventController(TimeseriesEventService eventService) {
        this.eventService = eventService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> listEvents() {
        eventService.listEvents(null);
        return returnSuccess("event list query success");
    }

    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> listEventsByJson(@RequestBody EventQueryRequest request) {
        eventService.listEvents(request);
        return returnSuccess("event list query success");
    }

    @GetMapping(value = "/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> getEventDetail(@PathVariable String eventId) {
        eventService.getEventDetail(eventId);
        return returnSuccess("event detail query success");
    }

    @PostMapping(value = "/detail", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> getEventDetailByJson(@RequestBody EventDetailQueryRequest request) {
        eventService.getEventDetail(request.getEventId());
        return returnSuccess("event detail query success");
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> saveEvent(@RequestBody EventSaveRequest request) {
        eventService.saveEvent(request);
        return returnSuccess("event save success");
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateEvent(@RequestBody EventSaveRequest request) {
        eventService.saveEvent(request);
        return returnSuccess("event update success");
    }
}

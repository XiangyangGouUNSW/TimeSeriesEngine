package com.sfkg.timeseries.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesDataFileMapper {

    private final Object fileLock = new Object();
    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    private final Path dataFile;

    public TimeseriesDataFileMapper(
            @Value("${timeseries.local-data-file:data/timeseries-data.jsonl}") String dataFilePath) {
        this.dataFile = Paths.get(dataFilePath).toAbsolutePath().normalize();
    }

    public void appendDataPoints(Collection<TimeseriesDataPoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }
        synchronized (fileLock) {
            try {
                Path parent = dataFile.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                List<String> lines = points.stream()
                        .map(this::writeAsJsonLine)
                        .collect(Collectors.toList());
                Files.write(
                        dataFile,
                        lines,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException exception) {
                throw new BusinessException("write local timeseries data file failed");
            }
        }
    }

    public List<TimeseriesDataPoint> selectByCondition(HistoryDataQueryRequest request) {
        synchronized (fileLock) {
            if (!Files.exists(dataFile)) {
                return List.of();
            }
            try {
                return Files.readAllLines(dataFile, StandardCharsets.UTF_8).stream()
                        .filter(line -> line != null && !line.isBlank())
                        .map(this::readJsonLine)
                        .filter(point -> matches(request, point))
                        .sorted(Comparator
                                .comparing(TimeseriesDataPoint::getSequenceId, Comparator.nullsLast(String::compareTo))
                                .thenComparing(TimeseriesDataPoint::getTimestamp, Comparator.nullsLast(LocalDateTime::compareTo)))
                        .collect(Collectors.toList());
            } catch (IOException exception) {
                throw new BusinessException("read local timeseries data file failed");
            }
        }
    }

    private String writeAsJsonLine(TimeseriesDataPoint point) {
        try {
            return objectMapper.writeValueAsString(point);
        } catch (IOException exception) {
            throw new BusinessException("serialize timeseries data point failed");
        }
    }

    private TimeseriesDataPoint readJsonLine(String line) {
        try {
            return objectMapper.readValue(line, TimeseriesDataPoint.class);
        } catch (IOException exception) {
            throw new BusinessException("parse local timeseries data file failed");
        }
    }

    private boolean matches(HistoryDataQueryRequest request, TimeseriesDataPoint point) {
        if (point == null) {
            return false;
        }
        if (request == null) {
            return true;
        }
        if (request.getSequenceId() != null && !request.getSequenceId().equals(point.getSequenceId())) {
            return false;
        }
        if (request.getProjectId() != null && !request.getProjectId().equals(point.getProjectId())) {
            return false;
        }
        if (request.getStartTime() != null && isBefore(point.getTimestamp(), request.getStartTime())) {
            return false;
        }
        return request.getEndTime() == null || !isAfter(point.getTimestamp(), request.getEndTime());
    }

    private boolean isBefore(LocalDateTime timestamp, LocalDateTime startTime) {
        return timestamp == null || timestamp.isBefore(startTime);
    }

    private boolean isAfter(LocalDateTime timestamp, LocalDateTime endTime) {
        return timestamp != null && timestamp.isAfter(endTime);
    }
}

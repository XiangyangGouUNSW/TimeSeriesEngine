package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.config.DataIngestProperties;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class DataIngestClient {

    private final DataIngestProperties properties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public DataIngestClient(DataIngestProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newHttpClient();
    }

    public boolean isEnabled() {
        return properties.isEnabled();
    }

    public String insert(DataIngestInsertPayload payload) {
        if (!isEnabled()) {
            return "";
        }
        HttpRequest request = HttpRequest.newBuilder(insertUri())
                .timeout(Duration.ofSeconds(Math.max(1, properties.getTimeoutSeconds())))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(payload), StandardCharsets.UTF_8))
                .build();
        return send(request);
    }

    public String health() {
        HttpRequest request = HttpRequest.newBuilder(healthUri())
                .timeout(Duration.ofSeconds(Math.max(1, properties.getTimeoutSeconds())))
                .GET()
                .build();
        return send(request);
    }

    private String send(HttpRequest request) {
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() >= 400) {
                throw new BusinessException("DataIngest request failed: status=" + response.statusCode());
            }
            return response.body();
        } catch (IOException exception) {
            throw new BusinessException("DataIngest request failed: " + exception.getMessage());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new BusinessException("DataIngest request interrupted");
        }
    }

    private URI insertUri() {
        return URI.create(trimEndpoint() + "/insert");
    }

    private URI healthUri() {
        return URI.create(trimEndpoint() + "/health");
    }

    private String trimEndpoint() {
        String endpoint = properties.getEndpoint();
        if (endpoint == null || endpoint.isBlank()) {
            endpoint = "http://127.0.0.1:8006";
        }
        while (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        return endpoint;
    }

    private String toJson(DataIngestInsertPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new BusinessException("serialize DataIngest request failed");
        }
    }
}

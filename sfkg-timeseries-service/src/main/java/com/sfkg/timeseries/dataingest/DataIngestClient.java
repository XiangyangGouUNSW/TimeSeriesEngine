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

    public DataIngestInsertResponse insert(DataIngestInsertPayload payload) {
        if (!isEnabled()) {
            return new DataIngestInsertResponse();
        }
        HttpRequest request = HttpRequest.newBuilder(insertUri())
                .timeout(Duration.ofSeconds(Math.max(1, properties.getTimeoutSeconds())))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(payload), StandardCharsets.UTF_8))
                .build();
        return parseInsertResponse(sendRaw(request));
    }

    public String health() {
        HttpRequest request = HttpRequest.newBuilder(healthUri())
                .timeout(Duration.ofSeconds(Math.max(1, properties.getTimeoutSeconds())))
                .GET()
                .build();
        return send(request);
    }

    private String send(HttpRequest request) {
        HttpResponse<String> response = sendRaw(request);
        if (response.statusCode() >= 400) {
            throw new BusinessException("DataIngest request failed: status="
                    + response.statusCode()
                    + ", body="
                    + response.body());
        }
        return response.body();
    }

    private HttpResponse<String> sendRaw(HttpRequest request) {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
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

    private DataIngestInsertResponse parseInsertResponse(HttpResponse<String> httpResponse) {
        String body = httpResponse.body();
        try {
            DataIngestInsertResponse response = objectMapper.readValue(body, DataIngestInsertResponse.class);
            if (!response.isSuccess()) {
                throw new BusinessException("DataIngest insert failed: status="
                        + httpResponse.statusCode()
                        + ", "
                        + response.summary());
            }
            return response;
        } catch (JsonProcessingException exception) {
            throw new BusinessException("parse DataIngest insert response failed: status="
                    + httpResponse.statusCode()
                    + ", body="
                    + body);
        }
    }
}

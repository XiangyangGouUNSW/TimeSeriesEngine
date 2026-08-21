package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
                .timeout(Duration.ofMillis(Math.max(1, properties.getTimeoutMillis())))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(payload), StandardCharsets.UTF_8))
                .build();
        return parseInsertResponse(sendRaw(request));
    }

    public DataIngestInsertResponse update(DataIngestInsertPayload payload) {
        if (!isEnabled()) {
            return new DataIngestInsertResponse();
        }
        HttpRequest request = HttpRequest.newBuilder(updateUri())
                .timeout(Duration.ofMillis(Math.max(1, properties.getTimeoutMillis())))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(payload), StandardCharsets.UTF_8))
                .build();
        return parseInsertResponse(sendRaw(request));
    }

    public DataIngestRecordsResponse queryRecords(String databaseName, String tableName) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("db_name", databaseName);
        payload.put("table_name", tableName);
        HttpRequest request = HttpRequest.newBuilder(recordsUri())
                .timeout(Duration.ofMillis(Math.max(1, properties.getTimeoutMillis())))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString(), StandardCharsets.UTF_8))
                .build();
        return parseRecordsResponse(sendRaw(request));
    }

    public String health() {
        HttpRequest request = HttpRequest.newBuilder(healthUri())
                .timeout(Duration.ofMillis(Math.max(1, properties.getTimeoutMillis())))
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

    private URI updateUri() {
        return URI.create(trimEndpoint() + "/update");
    }

    private URI healthUri() {
        return URI.create(trimEndpoint() + "/health");
    }

    private URI recordsUri() {
        return URI.create(trimEndpoint() + "/records");
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

    private DataIngestRecordsResponse parseRecordsResponse(HttpResponse<String> httpResponse) {
        String body = httpResponse.body();
        try {
            JsonNode root = objectMapper.readTree(body);
            if (httpResponse.statusCode() >= 400 || !root.path("success").asBoolean(false)) {
                String error = root.path("error").asText("unknown DataIngest records error");
                throw new BusinessException("DataIngest records query failed: status="
                        + httpResponse.statusCode() + ", " + error);
            }
            return objectMapper.treeToValue(root, DataIngestRecordsResponse.class);
        } catch (JsonProcessingException exception) {
            throw new BusinessException("parse DataIngest records response failed: status="
                    + httpResponse.statusCode() + ", body=" + body);
        }
    }
}

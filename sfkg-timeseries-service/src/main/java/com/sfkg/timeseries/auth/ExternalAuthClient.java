package com.sfkg.timeseries.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.CredentialsExpiredException;
import org.springframework.stereotype.Component;

@Component
public class ExternalAuthClient {

    private final SessionAuthProperties properties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public ExternalAuthClient(SessionAuthProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(timeout())
                .build();
    }

    public ExternalAuthAccount authenticate(String username, String password) {
        String token = login(username, password);
        String userId = findExactUserId(token, username);
        return getUser(token, userId, username);
    }

    private String login(String username, String password) {
        String body;
        try {
            body = objectMapper.writeValueAsString(Map.of(
                    "username", username,
                    "password", password));
        } catch (JsonProcessingException exception) {
            throw new AuthenticationServiceException("cannot serialize authentication request", exception);
        }

        HttpRequest request = HttpRequest.newBuilder(uri("/api/v1/auth/login"))
                .timeout(timeout())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        JsonNode root = sendJson(request, true);
        String token = root.path("data").path("token").asText("").trim();
        if (!token.startsWith("gbusr_")) {
            throw new AuthenticationServiceException("authentication API returned an invalid user token");
        }
        return token;
    }

    private String findExactUserId(String token, String username) {
        String query = URLEncoder.encode(username, StandardCharsets.UTF_8);
        HttpRequest request = authorizedGet(
                "/api/v1/system/users?q=" + query,
                token);
        JsonNode root = sendJson(request, false);
        for (JsonNode candidate : extractUsers(root.path("data"))) {
            if (username.equals(candidate.path("username").asText())) {
                String userId = text(candidate, "id", "userId");
                if (!userId.isBlank()) {
                    return userId;
                }
            }
        }
        throw new BadCredentialsException("authenticated user cannot be resolved exactly");
    }

    private ExternalAuthAccount getUser(String token, String userId, String expectedUsername) {
        HttpRequest request = authorizedGet(
                "/api/v1/system/users/" + encodePathSegment(userId),
                token);
        JsonNode data = sendJson(request, false).path("data");
        String actualUserId = text(data, "id", "userId");
        String actualUsername = data.path("username").asText("");
        String status = data.path("status").asText("");
        if (!expectedUsername.equals(actualUsername)
                || (!actualUserId.isBlank() && !userId.equals(actualUserId))) {
            throw new BadCredentialsException("authenticated user details do not match");
        }
        if (!"active".equalsIgnoreCase(status)) {
            throw new BadCredentialsException("user account is not active");
        }

        Set<String> roleCodes = new LinkedHashSet<>();
        JsonNode roles = data.path("roles");
        if (roles.isArray()) {
            for (JsonNode role : roles) {
                String code = role.path("code").asText("").trim();
                if (!code.isEmpty()) {
                    roleCodes.add(code);
                }
            }
        }
        return new ExternalAuthAccount(userId, actualUsername, Set.copyOf(roleCodes), token);
    }

    private HttpRequest authorizedGet(String path, String token) {
        return HttpRequest.newBuilder(uri(path))
                .timeout(timeout())
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
    }

    private JsonNode sendJson(HttpRequest request, boolean loginRequest) {
        HttpResponse<String> response;
        try {
            response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException exception) {
            throw new AuthenticationServiceException("authentication API request failed", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AuthenticationServiceException("authentication API request interrupted", exception);
        }

        int status = response.statusCode();
        if (status == 401) {
            if (loginRequest) {
                throw new BadCredentialsException("username or password is incorrect");
            }
            throw new CredentialsExpiredException("authentication token is invalid; login again");
        }
        if (status == 403) {
            throw new BadCredentialsException("user cannot query role information");
        }
        if (status >= 500) {
            throw new AuthenticationServiceException(
                    "authentication API unavailable: status=" + status);
        }
        if (status >= 400) {
            throw new BadCredentialsException(
                    "authentication API rejected request: status=" + status);
        }

        try {
            return objectMapper.readTree(response.body());
        } catch (JsonProcessingException exception) {
            throw new AuthenticationServiceException(
                    "authentication API returned invalid JSON", exception);
        }
    }

    private List<JsonNode> extractUsers(JsonNode data) {
        List<JsonNode> users = new ArrayList<>();
        if (data.isArray()) {
            data.forEach(users::add);
            return users;
        }
        for (String field : List.of("items", "records", "content", "list", "users")) {
            JsonNode values = data.path(field);
            if (values.isArray()) {
                values.forEach(users::add);
                return users;
            }
        }
        if (data.isObject() && data.has("username")) {
            users.add(data);
        }
        return users;
    }

    private String text(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            String value = node.path(fieldName).asText("").trim();
            if (!value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private URI uri(String path) {
        return URI.create(trimBaseUrl() + path);
    }

    private String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private String trimBaseUrl() {
        String value = properties.getAuthApiUrl();
        if (value == null || value.isBlank()) {
            throw new AuthenticationServiceException("timeseries.auth.auth-api-url is required");
        }
        value = value.trim();
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private Duration timeout() {
        return Duration.ofMillis(Math.max(1, properties.getAuthApiTimeoutMillis()));
    }
}

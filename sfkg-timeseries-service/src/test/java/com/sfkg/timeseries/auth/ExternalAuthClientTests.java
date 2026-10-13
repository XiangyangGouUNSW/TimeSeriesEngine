package com.sfkg.timeseries.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.CredentialsExpiredException;

class ExternalAuthClientTests {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void authenticatesAndLoadsRolesWithTheUserToken() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/auth/login", exchange -> respond(exchange, 200,
                "{\"data\":{\"token\":\"gbusr_test\"}}"));
        server.createContext("/api/v1/system/users", exchange -> {
            assertEquals("Bearer gbusr_test", exchange.getRequestHeaders().getFirst("Authorization"));
            if (exchange.getRequestURI().getPath().endsWith("/u-1")) {
                respond(exchange, 200, """
                        {"data":{"id":"u-1","username":"operator","status":"active",
                        "roles":[{"code":"Reader"},{"code":"Operator"}]}}
                        """);
            } else {
                assertEquals("q=operator", exchange.getRequestURI().getRawQuery());
                respond(exchange, 200,
                        "{\"data\":{\"items\":[{\"id\":\"u-1\",\"username\":\"operator\"}]}}" );
            }
        });
        server.start();

        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setAuthApiUrl("http://127.0.0.1:" + server.getAddress().getPort());
        ExternalAuthClient client = new ExternalAuthClient(properties, new ObjectMapper());

        ExternalAuthAccount account = client.authenticate("operator", "secret");

        assertEquals("u-1", account.userId());
        assertEquals("operator", account.username());
        assertEquals("gbusr_test", account.token());
        assertTrue(account.roleCodes().containsAll(java.util.Set.of("Reader", "Operator")));
    }

    @Test
    void requiresLoginAgainWhenUserTokenIsRejected() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/auth/login", exchange -> respond(exchange, 200,
                "{\"data\":{\"token\":\"gbusr_expired\"}}"));
        server.createContext("/api/v1/system/users", exchange -> respond(exchange, 401,
                "{\"error\":{\"code\":\"UNAUTHORIZED\",\"message\":\"token expired\"}}"));
        server.start();

        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setAuthApiUrl("http://127.0.0.1:" + server.getAddress().getPort());
        ExternalAuthClient client = new ExternalAuthClient(properties, new ObjectMapper());

        assertThrows(
                CredentialsExpiredException.class,
                () -> client.authenticate("operator", "secret"));
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}

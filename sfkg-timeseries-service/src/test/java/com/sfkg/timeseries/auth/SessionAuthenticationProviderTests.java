package com.sfkg.timeseries.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

class SessionAuthenticationProviderTests {

    @Test
    void bootstrapUserAuthenticatesWithAllPermissions(@TempDir Path tempDir) throws Exception {
        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setUserFile(tempDir.resolve("users.json").toString());
        properties.setBootstrapUsername("admin");
        properties.setBootstrapPassword("unit-test-password");
        PasswordEncoder encoder = new BCryptPasswordEncoder();
        SessionUserStore store = new SessionUserStore(new ObjectMapper(), properties, encoder);
        SessionAuthenticationProvider provider = new SessionAuthenticationProvider(store, encoder);

        var authentication = provider.authenticate(
                new UsernamePasswordAuthenticationToken("admin", "unit-test-password"));
        var user = (AuthenticatedUser) authentication.getPrincipal();

        assertEquals("u-admin", user.getUserId());
        assertEquals(3, user.getPermissions().size());
        assertTrue(user.getPermissions().contains(PermissionCode.HISTORY_DATA.name()));
        assertTrue(user.getPermissions().contains(PermissionCode.CONFIG_INFO.name()));
        assertTrue(user.getPermissions().contains(PermissionCode.TASK_INFO.name()));
        assertTrue(Files.exists(tempDir.resolve("users.json")));
        assertTrue(Files.readString(tempDir.resolve("users.json")).contains("passwordHash"));
        assertTrue(!Files.readString(tempDir.resolve("users.json")).contains("unit-test-password"));
    }

    @Test
    void invalidPasswordIsRejected(@TempDir Path tempDir) {
        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setUserFile(tempDir.resolve("users.json").toString());
        properties.setBootstrapUsername("admin");
        properties.setBootstrapPassword("unit-test-password");
        PasswordEncoder encoder = new BCryptPasswordEncoder();
        SessionUserStore store = new SessionUserStore(new ObjectMapper(), properties, encoder);
        SessionAuthenticationProvider provider = new SessionAuthenticationProvider(store, encoder);

        assertThrows(BadCredentialsException.class, () -> provider.authenticate(
                new UsernamePasswordAuthenticationToken("admin", "wrong-password")));
    }

    @Test
    void existingBootstrapUserWithoutPermissionsIsMigrated(@TempDir Path tempDir) throws Exception {
        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setUserFile(tempDir.resolve("users.json").toString());
        properties.setBootstrapUsername("admin");
        properties.setBootstrapPassword("unit-test-password");
        PasswordEncoder encoder = new BCryptPasswordEncoder();
        ObjectMapper objectMapper = new ObjectMapper();

        SessionUserAccount account = new SessionUserAccount();
        account.setUserId("u-admin");
        account.setUsername("admin");
        account.setPasswordHash(encoder.encode("unit-test-password"));
        account.setPermissions(new LinkedHashSet<>());
        objectMapper.writeValue(tempDir.resolve("users.json").toFile(), java.util.List.of(account));

        SessionUserStore store = new SessionUserStore(objectMapper, properties, encoder);
        SessionAuthenticationProvider provider = new SessionAuthenticationProvider(store, encoder);
        var authentication = provider.authenticate(
                new UsernamePasswordAuthenticationToken("admin", "unit-test-password"));

        assertEquals(3, ((AuthenticatedUser) authentication.getPrincipal()).getPermissions().size());
    }
}

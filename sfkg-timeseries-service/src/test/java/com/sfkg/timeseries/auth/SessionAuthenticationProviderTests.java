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
        properties.setBootstrapPassword("admin123");
        PasswordEncoder encoder = new BCryptPasswordEncoder();
        SessionUserStore store = new SessionUserStore(new ObjectMapper(), properties, encoder);
        SessionAuthenticationProvider provider = new SessionAuthenticationProvider(store, encoder);

        var authentication = provider.authenticate(
                new UsernamePasswordAuthenticationToken("admin", "admin123"));
        var user = (AuthenticatedUser) authentication.getPrincipal();

        assertEquals("u-admin", user.getUserId());
        assertEquals(3, user.getPermissions().size());
        assertTrue(user.getPermissions().contains(PermissionCode.HISTORY_DATA.name()));
        assertTrue(user.getPermissions().contains(PermissionCode.CONFIG_INFO.name()));
        assertTrue(user.getPermissions().contains(PermissionCode.TASK_INFO.name()));
        assertTrue(Files.exists(tempDir.resolve("users.json")));
        assertTrue(Files.readString(tempDir.resolve("users.json")).contains("passwordHash"));
        assertTrue(!Files.readString(tempDir.resolve("users.json")).contains("admin123"));
    }

    @Test
    void invalidPasswordIsRejected(@TempDir Path tempDir) {
        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setUserFile(tempDir.resolve("users.json").toString());
        properties.setBootstrapUsername("admin");
        properties.setBootstrapPassword("admin123");
        PasswordEncoder encoder = new BCryptPasswordEncoder();
        SessionUserStore store = new SessionUserStore(new ObjectMapper(), properties, encoder);
        SessionAuthenticationProvider provider = new SessionAuthenticationProvider(store, encoder);

        assertThrows(BadCredentialsException.class, () -> provider.authenticate(
                new UsernamePasswordAuthenticationToken("admin", "wrong-password")));
    }
}

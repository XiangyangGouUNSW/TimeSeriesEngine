package com.sfkg.timeseries.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
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

    @Test
    void externalModeMapsRolesAndKeepsTokenInAuthenticationDetails() {
        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setUseAuthApi(true);
        SessionUserStore userStore = mock(SessionUserStore.class);
        PasswordEncoder encoder = mock(PasswordEncoder.class);
        ExternalAuthClient client = mock(ExternalAuthClient.class);
        RolePermissionMapper mapper = mock(RolePermissionMapper.class);
        when(client.authenticate("operator", "secret"))
                .thenReturn(new ExternalAuthAccount(
                        "u-1",
                        "operator",
                        Set.of("Reader"),
                        "gbusr_test"));
        when(mapper.resolvePermissions(Set.of("Reader")))
                .thenReturn(Set.of(PermissionCode.HISTORY_DATA.name()));
        SessionAuthenticationProvider provider = new SessionAuthenticationProvider(
                userStore,
                encoder,
                properties,
                client,
                mapper);

        var authentication = provider.authenticate(
                new UsernamePasswordAuthenticationToken("operator", "secret"));
        var user = (AuthenticatedUser) authentication.getPrincipal();

        assertEquals("u-1", user.getUserId());
        assertEquals(Set.of(PermissionCode.HISTORY_DATA.name()), user.getPermissions());
        assertEquals(
                "gbusr_test",
                ((ExternalAuthenticationDetails) authentication.getDetails()).token());
    }
}

package com.sfkg.timeseries.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.DefaultResourceLoader;

class RolePermissionMapperTests {

    @Test
    void combinesPermissionsFromAllUserRoles(@TempDir Path tempDir) throws Exception {
        Path mappingFile = tempDir.resolve("roles.json");
        Files.writeString(mappingFile, """
                {
                  "Reader": ["HISTORY_DATA"],
                  "Operator": ["CONFIG_INFO", "TASK_INFO"]
                }
                """);
        SessionAuthProperties properties = new SessionAuthProperties();
        properties.setRolePermissionFile(mappingFile.toUri().toString());
        RolePermissionMapper mapper = new RolePermissionMapper(
                properties,
                new ObjectMapper(),
                new DefaultResourceLoader());

        Set<String> permissions = mapper.resolvePermissions(Set.of("Reader", "Operator"));

        assertEquals(Set.of("HISTORY_DATA", "CONFIG_INFO", "TASK_INFO"), permissions);
    }
}

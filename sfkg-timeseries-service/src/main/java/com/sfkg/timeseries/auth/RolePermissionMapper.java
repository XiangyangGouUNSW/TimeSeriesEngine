package com.sfkg.timeseries.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.stereotype.Component;

@Component
public class RolePermissionMapper {

    private final SessionAuthProperties properties;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;

    public RolePermissionMapper(
            SessionAuthProperties properties,
            ObjectMapper objectMapper,
            ResourceLoader resourceLoader) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
    }

    public Set<String> resolvePermissions(Set<String> roleCodes) {
        Map<String, List<String>> mappings = loadMappings();
        Set<String> permissions = new LinkedHashSet<>();
        for (String roleCode : roleCodes == null ? Set.<String>of() : roleCodes) {
            for (String permission : mappings.getOrDefault(roleCode, List.of())) {
                try {
                    permissions.add(PermissionCode.valueOf(permission).name());
                } catch (IllegalArgumentException exception) {
                    throw new AuthenticationServiceException(
                            "unknown permission in role mapping: " + permission,
                            exception);
                }
            }
        }
        return Set.copyOf(permissions);
    }

    private Map<String, List<String>> loadMappings() {
        String location = properties.getRolePermissionFile();
        if (location == null || location.isBlank()) {
            throw new AuthenticationServiceException(
                    "timeseries.auth.role-permission-file is required");
        }
        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            throw new AuthenticationServiceException(
                    "role permission mapping file not found: " + location);
        }
        try (InputStream input = resource.getInputStream()) {
            return objectMapper.readValue(
                    input,
                    new TypeReference<Map<String, List<String>>>() {
                    });
        } catch (IOException exception) {
            throw new AuthenticationServiceException(
                    "cannot read role permission mapping file: " + location,
                    exception);
        }
    }
}

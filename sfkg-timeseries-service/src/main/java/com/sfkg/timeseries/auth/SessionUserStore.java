package com.sfkg.timeseries.auth;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.common.BusinessException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Repository;

/** Local user store for the first runnable session-authentication version. */
@Repository
public class SessionUserStore {

    private final Object fileLock = new Object();
    private final Path userFile;
    private final ObjectMapper objectMapper;
    private final SessionAuthProperties properties;
    private final PasswordEncoder passwordEncoder;

    public SessionUserStore(
            ObjectMapper objectMapper,
            SessionAuthProperties properties,
            PasswordEncoder passwordEncoder) {
        this.userFile = Paths.get(properties.getUserFile()).toAbsolutePath().normalize();
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.passwordEncoder = passwordEncoder;
    }

    public Optional<SessionUserAccount> findByUsername(String username) {
        if (username == null || username.isBlank()) {
            return Optional.empty();
        }
        synchronized (fileLock) {
            List<SessionUserAccount> accounts = readAllUnlocked();
            ensureBootstrapUserUnlocked(accounts);
            return accounts.stream()
                    .filter(account -> username.equals(account.getUsername()))
                    .findFirst();
        }
    }

    private void ensureBootstrapUserUnlocked(List<SessionUserAccount> accounts) {
        String username = properties.getBootstrapUsername();
        if (username == null || username.isBlank()) {
            throw new BusinessException("bootstrap authentication user is not configured");
        }
        SessionUserAccount existingBootstrap = accounts.stream()
                .filter(account -> username.equals(account.getUsername()))
                .findFirst()
                .orElse(null);
        if (existingBootstrap != null) {
            // Migrate an administrator file created by the initial auth draft.
            if (existingBootstrap.getPermissions() == null
                    || existingBootstrap.getPermissions().isEmpty()) {
                existingBootstrap.setPermissions(allPermissions());
                writeAllUnlocked(accounts);
            }
            return;
        }

        String password = properties.getBootstrapPassword();
        if (password == null || password.isBlank()) {
            throw new BusinessException(
                    "bootstrap authentication password is not configured; set TIMESERIES_AUTH_BOOTSTRAP_PASSWORD");
        }

        SessionUserAccount admin = new SessionUserAccount();
        admin.setUserId("u-admin");
        admin.setUsername(username);
        admin.setPasswordHash(passwordEncoder.encode(password));
        admin.setEnabled(true);
        admin.setPermissions(allPermissions());
        accounts.add(admin);
        writeAllUnlocked(accounts);
    }

    private LinkedHashSet<String> allPermissions() {
        LinkedHashSet<String> permissions = new LinkedHashSet<>();
        for (PermissionCode permission : PermissionCode.values()) {
            permissions.add(permission.name());
        }
        return permissions;
    }

    private List<SessionUserAccount> readAllUnlocked() {
        if (!Files.exists(userFile)) {
            return new ArrayList<>();
        }
        try {
            String content = Files.readString(userFile, StandardCharsets.UTF_8);
            if (content == null || content.isBlank()) {
                return new ArrayList<>();
            }
            JavaType listType = objectMapper.getTypeFactory()
                    .constructCollectionType(List.class, SessionUserAccount.class);
            return new ArrayList<>(objectMapper.readValue(content, listType));
        } catch (IOException exception) {
            throw new BusinessException("read session user file failed: " + userFile.getFileName());
        }
    }

    private void writeAllUnlocked(List<SessionUserAccount> accounts) {
        try {
            Path parent = userFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(userFile.toFile(), accounts);
        } catch (IOException exception) {
            throw new BusinessException("write session user file failed: " + userFile.getFileName());
        }
    }
}

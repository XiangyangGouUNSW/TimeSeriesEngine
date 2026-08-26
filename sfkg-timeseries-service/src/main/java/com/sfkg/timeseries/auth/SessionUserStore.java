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
        String password = properties.getBootstrapPassword();
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new BusinessException("bootstrap authentication user is not configured");
        }
        boolean exists = accounts.stream().anyMatch(account -> username.equals(account.getUsername()));
        if (exists) {
            return;
        }

        SessionUserAccount admin = new SessionUserAccount();
        admin.setUserId("u-admin");
        admin.setUsername(username);
        admin.setPasswordHash(passwordEncoder.encode(password));
        admin.setEnabled(true);
        admin.setPermissions(new LinkedHashSet<>());
        for (PermissionCode permission : PermissionCode.values()) {
            admin.getPermissions().add(permission.name());
        }
        accounts.add(admin);
        writeAllUnlocked(accounts);
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

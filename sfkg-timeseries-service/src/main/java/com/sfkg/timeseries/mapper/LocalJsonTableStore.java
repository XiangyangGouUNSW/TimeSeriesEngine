package com.sfkg.timeseries.mapper;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sfkg.timeseries.common.BusinessException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

class LocalJsonTableStore<T> {

    private final Object fileLock = new Object();
    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    private final Path tableFile;
    private final Class<T> recordType;

    LocalJsonTableStore(String storeDir, String fileName, Class<T> recordType) {
        this(Paths.get(storeDir, fileName), recordType);
    }

    LocalJsonTableStore(Path tableFile, Class<T> recordType) {
        this.tableFile = tableFile.toAbsolutePath().normalize();
        this.recordType = recordType;
    }

    List<T> readAll() {
        synchronized (fileLock) {
            return readAllUnlocked();
        }
    }

    void writeAll(Collection<T> records) {
        synchronized (fileLock) {
            writeAllUnlocked(records == null ? List.of() : new ArrayList<>(records));
        }
    }

    void upsert(Predicate<T> sameBusinessKey, T entity) {
        if (entity == null) {
            return;
        }
        synchronized (fileLock) {
            List<T> records = readAllUnlocked();
            records.removeIf(sameBusinessKey);
            records.add(entity);
            writeAllUnlocked(records);
        }
    }

    void update(Predicate<T> sameBusinessKey, Consumer<T> updater) {
        synchronized (fileLock) {
            List<T> records = readAllUnlocked();
            records.stream()
                    .filter(sameBusinessKey)
                    .findFirst()
                    .ifPresent(updater);
            writeAllUnlocked(records);
        }
    }

    private List<T> readAllUnlocked() {
        if (!Files.exists(tableFile)) {
            return new ArrayList<>();
        }
        try {
            String content = Files.readString(tableFile, StandardCharsets.UTF_8);
            if (content == null || content.isBlank()) {
                return new ArrayList<>();
            }
            JavaType listType = objectMapper.getTypeFactory().constructCollectionType(List.class, recordType);
            return new ArrayList<>(objectMapper.readValue(content, listType));
        } catch (IOException exception) {
            throw new BusinessException("read local json table failed: " + tableFile.getFileName());
        }
    }

    private void writeAllUnlocked(Collection<T> records) {
        try {
            Path parent = tableFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tableFile.toFile(), records);
        } catch (IOException exception) {
            throw new BusinessException("write local json table failed: " + tableFile.getFileName());
        }
    }
}

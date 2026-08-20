package com.sfkg.timeseries.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sfkg.timeseries.entity.TimeseriesCategory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalJsonTableStoreTests {

    @Test
    void unknownFieldsAreIgnoredWhenReadingLocalSnapshot(@TempDir Path tempDir) throws Exception {
        Path tableFile = tempDir.resolve("timeseries-category.json");
        Files.writeString(tableFile, """
                [{
                  "projectId": "project-a",
                  "categoryId": "cat001",
                  "categoryName": "Temperature",
                  "futureField": "ignored"
                }]
                """, StandardCharsets.UTF_8);

        LocalJsonTableStore<TimeseriesCategory> store = new LocalJsonTableStore<>(
                tableFile, TimeseriesCategory.class);

        assertEquals("cat001", store.readAll().get(0).getCategoryId());
        assertEquals("Temperature", store.readAll().get(0).getCategoryName());
    }
}

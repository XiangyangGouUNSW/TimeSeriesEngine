package com.sfkg.timeseries.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "timeseries.data-ingest")
public class DataIngestProperties {

    private boolean enabled = false;
    private boolean readFromGstore = false;
    private boolean fallbackToLocal = true;
    private String endpoint = "http://127.0.0.1:8006";
    private String database = "ett_system";
    private String namespace = "http://gbuilder.org/knowledge/";
    private long timeoutMillis = 500;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isReadFromGstore() {
        return readFromGstore;
    }

    public void setReadFromGstore(boolean readFromGstore) {
        this.readFromGstore = readFromGstore;
    }

    public boolean isFallbackToLocal() {
        return fallbackToLocal;
    }

    public void setFallbackToLocal(boolean fallbackToLocal) {
        this.fallbackToLocal = fallbackToLocal;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Resolve the gStore database dedicated to a project. The project id is
     * used only for routing; it is not part of the entity payload.
     */
    public String databaseForProject(String projectId) {
        if (projectId == null || projectId.isBlank()) {
            throw new IllegalArgumentException("projectId must not be blank when resolving a gStore database");
        }
        String normalized = projectId.trim();
        if (!normalized.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
            throw new IllegalArgumentException("projectId contains unsupported characters: " + projectId);
        }
        return database + "_" + normalized;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

}

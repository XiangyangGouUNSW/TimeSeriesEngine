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

    /**
     * Resolve the gStore database dedicated to a project. The database name is
     * the project id itself; there is no shared prefix.
     */
    public String databaseForProject(String projectId) {
        if (projectId == null || projectId.isBlank()) {
            throw new IllegalArgumentException("projectId must not be blank when resolving a gStore database");
        }
        String normalized = projectId.trim();
        if (!normalized.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
            throw new IllegalArgumentException("projectId contains unsupported characters: " + projectId);
        }
        return normalized;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

}

package com.sfkg.timeseries.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "timeseries.data-ingest")
public class DataIngestProperties {

    private boolean enabled = false;
    private String endpoint = "http://127.0.0.1:8006";
    private String database = "ship_power_system";
    private int timeoutSeconds = 5;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
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

    /**
     * Resolve the gStore database dedicated to a project. The project id is
     * used only for routing; it is not part of the entity payload.
     */
    public String databaseForProject(String projectId) {
        if (projectId == null || projectId.isBlank()) {
            return database;
        }
        String normalized = projectId.trim().replaceAll("[^A-Za-z0-9_-]", "_");
        return database + "_" + normalized;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

}

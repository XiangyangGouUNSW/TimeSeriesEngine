package com.sfkg.timeseries.dataingest;

import java.util.LinkedHashMap;
import java.util.Map;

public class DataIngestEntityPayload {

    private String name;
    private String type;
    private String description;
    private Map<String, Object> properties = new LinkedHashMap<>();

    public DataIngestEntityPayload() {
    }

    public DataIngestEntityPayload(String name, String type, String description, Map<String, Object> properties) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.properties = properties == null ? new LinkedHashMap<>() : properties;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}

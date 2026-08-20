package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataIngestRecord {

    @JsonProperty("business_key")
    private String businessKey;

    private JsonNode record;

    public String getBusinessKey() {
        return businessKey;
    }

    public void setBusinessKey(String businessKey) {
        this.businessKey = businessKey;
    }

    public JsonNode getRecord() {
        return record;
    }

    public void setRecord(JsonNode record) {
        this.record = record;
    }
}

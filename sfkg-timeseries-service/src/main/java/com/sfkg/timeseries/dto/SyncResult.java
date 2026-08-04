package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class SyncResult {

    private boolean success;
    private String message;

    public static SyncResult success() {
        return of(true, "success");
    }

    public static SyncResult fail(String message) {
        return of(false, message);
    }

    public static SyncResult of(boolean success, String message) {
        SyncResult result = new SyncResult();
        result.setSuccess(success);
        result.setMessage(message);
        return result;
    }
}

package com.sfkg.timeseries.common;

import com.sfkg.timeseries.dto.SyncResult;

/** Converts a negative downstream acknowledgement into a failed REST operation. */
public final class DownstreamSyncValidator {

    private DownstreamSyncValidator() {
    }

    public static void requireSuccess(String downstream, SyncResult result) {
        if (result == null || !result.isSuccess()) {
            String detail = result == null || result.getMessage() == null || result.getMessage().isBlank()
                    ? "no acknowledgement returned"
                    : result.getMessage();
            throw new BusinessException(downstream + " rejected the configuration: " + detail);
        }
    }
}

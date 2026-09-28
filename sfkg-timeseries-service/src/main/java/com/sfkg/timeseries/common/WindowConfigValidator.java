package com.sfkg.timeseries.common;

import com.sfkg.timeseries.dto.WindowConfigSaveRequest;

/** Validation shared by the window configuration REST entry point. */
public final class WindowConfigValidator {

    private static final long MAX_WINDOW_SIZE_MS = 31L * 24 * 60 * 60 * 1_000;

    private WindowConfigValidator() {
    }

    public static void validate(WindowConfigSaveRequest request) {
        if (request == null || request.getWindowSizeMs() <= 0) {
            throw new BusinessException("windowSizeMs must be positive");
        }
        if (request.getWindowSizeMs() > MAX_WINDOW_SIZE_MS) {
            throw new BusinessException("windowSizeMs exceeds maximum: " + MAX_WINDOW_SIZE_MS);
        }
    }
}

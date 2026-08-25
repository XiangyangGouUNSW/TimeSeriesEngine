package com.sfkg.timeseries.common;

/** Validates the project identifier used as the service tenant boundary. */
public final class ProjectIdValidator {

    private static final String PROJECT_ID_PATTERN = "[A-Za-z0-9][A-Za-z0-9_-]{0,127}";

    private ProjectIdValidator() {
    }

    public static String require(String projectId) {
        if (projectId == null || projectId.isBlank()) {
            throw new BusinessException("projectId must not be blank");
        }
        String normalized = projectId.trim();
        if (!normalized.matches(PROJECT_ID_PATTERN)) {
            throw new BusinessException(
                    "projectId must start with a letter or digit and contain only letters, digits, '_' or '-'");
        }
        return normalized;
    }
}

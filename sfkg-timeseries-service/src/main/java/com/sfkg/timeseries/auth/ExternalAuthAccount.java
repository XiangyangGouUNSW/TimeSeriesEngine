package com.sfkg.timeseries.auth;

import java.util.Set;

public record ExternalAuthAccount(
        String userId,
        String username,
        Set<String> roleCodes,
        String token) {
}

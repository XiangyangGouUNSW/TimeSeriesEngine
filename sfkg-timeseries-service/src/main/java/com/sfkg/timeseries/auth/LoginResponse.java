package com.sfkg.timeseries.auth;

import java.util.Set;

public record LoginResponse(
        String userId,
        String username,
        Set<String> permissions) {
}

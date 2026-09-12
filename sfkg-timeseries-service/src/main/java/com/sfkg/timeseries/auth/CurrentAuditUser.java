package com.sfkg.timeseries.auth;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/** Resolves the trusted actor for persisted audit fields. */
public final class CurrentAuditUser {

    public static final String SYSTEM_USERNAME = "system";

    private CurrentAuditUser() {
    }

    /**
     * Uses the authenticated Session principal for requests. Background
     * callbacks without a user session are recorded explicitly as system.
     */
    public static String username() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null
                && authentication.isAuthenticated()
                && authentication.getPrincipal() instanceof AuthenticatedUser user
                && user.getUsername() != null
                && !user.getUsername().isBlank()) {
            return user.getUsername();
        }
        return SYSTEM_USERNAME;
    }
}

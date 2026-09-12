package com.sfkg.timeseries.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

class CurrentAuditUserTests {

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void usesAuthenticatedSessionUsername() {
        AuthenticatedUser user = new AuthenticatedUser("user-1", "operator-a", Set.of());
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(user, null, user.getAuthorities()));

        assertEquals("operator-a", CurrentAuditUser.username());
    }

    @Test
    void marksBackgroundCallsAsSystem() {
        assertEquals(CurrentAuditUser.SYSTEM_USERNAME, CurrentAuditUser.username());
    }
}

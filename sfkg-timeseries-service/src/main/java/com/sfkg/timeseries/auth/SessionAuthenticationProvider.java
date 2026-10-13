package com.sfkg.timeseries.auth;

import java.util.Optional;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class SessionAuthenticationProvider implements AuthenticationProvider {

    private final SessionUserStore userStore;
    private final PasswordEncoder passwordEncoder;
    private final SessionAuthProperties properties;
    private final ExternalAuthClient externalAuthClient;
    private final RolePermissionMapper rolePermissionMapper;

    @Autowired
    public SessionAuthenticationProvider(
            SessionUserStore userStore,
            PasswordEncoder passwordEncoder,
            SessionAuthProperties properties,
            ExternalAuthClient externalAuthClient,
            RolePermissionMapper rolePermissionMapper) {
        this.userStore = userStore;
        this.passwordEncoder = passwordEncoder;
        this.properties = properties;
        this.externalAuthClient = externalAuthClient;
        this.rolePermissionMapper = rolePermissionMapper;
    }

    // Kept for focused local-auth unit tests.
    public SessionAuthenticationProvider(SessionUserStore userStore, PasswordEncoder passwordEncoder) {
        this.userStore = userStore;
        this.passwordEncoder = passwordEncoder;
        this.properties = new SessionAuthProperties();
        this.externalAuthClient = null;
        this.rolePermissionMapper = null;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String username = authentication.getName();
        String password = authentication.getCredentials() == null
                ? ""
                : String.valueOf(authentication.getCredentials());
        if (properties.isUseAuthApi()) {
            return authenticateExternally(username, password);
        }

        Optional<SessionUserAccount> account = userStore.findByUsername(username);
        if (account.isEmpty()
                || !account.get().isEnabled()
                || !passwordEncoder.matches(password, account.get().getPasswordHash())) {
            throw new BadCredentialsException("username or password is incorrect");
        }

        SessionUserAccount value = account.get();
        AuthenticatedUser principal = new AuthenticatedUser(
                value.getUserId(), value.getUsername(), value.getPermissions());
        return new UsernamePasswordAuthenticationToken(
                principal,
                null,
                principal.getAuthorities());
    }

    private Authentication authenticateExternally(String username, String password) {
        if (externalAuthClient == null || rolePermissionMapper == null) {
            throw new IllegalStateException("external authentication components are unavailable");
        }
        ExternalAuthAccount account = externalAuthClient.authenticate(username, password);
        Set<String> permissions = rolePermissionMapper.resolvePermissions(account.roleCodes());
        if (permissions.isEmpty()) {
            throw new DisabledException("no timeseries permission is mapped for user roles");
        }

        AuthenticatedUser principal = new AuthenticatedUser(
                account.userId(),
                account.username(),
                permissions);
        UsernamePasswordAuthenticationToken result = new UsernamePasswordAuthenticationToken(
                principal,
                null,
                principal.getAuthorities());
        result.setDetails(new ExternalAuthenticationDetails(account.token()));
        return result;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}

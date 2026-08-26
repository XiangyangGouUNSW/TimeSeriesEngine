package com.sfkg.timeseries.auth;

import java.util.Optional;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class SessionAuthenticationProvider implements AuthenticationProvider {

    private final SessionUserStore userStore;
    private final PasswordEncoder passwordEncoder;

    public SessionAuthenticationProvider(SessionUserStore userStore, PasswordEncoder passwordEncoder) {
        this.userStore = userStore;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String username = authentication.getName();
        String password = authentication.getCredentials() == null
                ? ""
                : String.valueOf(authentication.getCredentials());
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

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}

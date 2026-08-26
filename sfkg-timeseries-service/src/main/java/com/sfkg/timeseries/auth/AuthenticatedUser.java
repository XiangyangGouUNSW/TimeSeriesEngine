package com.sfkg.timeseries.auth;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

public final class AuthenticatedUser implements UserDetails {

    private final String userId;
    private final String username;
    private final Set<String> permissions;
    private final Collection<? extends GrantedAuthority> authorities;

    public AuthenticatedUser(String userId, String username, Set<String> permissions) {
        this.userId = userId;
        this.username = username;
        this.permissions = Set.copyOf(permissions == null ? Set.of() : permissions);
        this.authorities = this.permissions.stream()
                .map(SimpleGrantedAuthority::new)
                .toList();
    }

    public String getUserId() {
        return userId;
    }

    public Set<String> getPermissions() {
        return new LinkedHashSet<>(permissions);
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return authorities;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}

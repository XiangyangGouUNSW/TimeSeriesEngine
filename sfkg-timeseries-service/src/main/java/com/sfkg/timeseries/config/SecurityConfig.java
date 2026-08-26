package com.sfkg.timeseries.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.auth.PermissionCode;
import com.sfkg.timeseries.auth.SessionAuthenticationProvider;
import com.sfkg.timeseries.common.ApiResult;
import java.nio.charset.StandardCharsets;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(SessionAuthenticationProvider provider) {
        return new ProviderManager(provider);
    }

    @Bean
    public SecurityContextRepository securityContextRepository() {
        return new HttpSessionSecurityContextRepository();
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            SessionAuthenticationProvider authenticationProvider,
            ObjectMapper objectMapper) throws Exception {
        http
                .authenticationProvider(authenticationProvider)
                .csrf(csrf -> csrf
                        .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
                        .ignoringRequestMatchers("/api/auth/login", "/api/auth/logout"))
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED))
                .exceptionHandling(exception -> exception
                        .authenticationEntryPoint((request, response, cause) ->
                                writeError(response, objectMapper, 401, "login required"))
                        .accessDeniedHandler((request, response, cause) ->
                                writeError(response, objectMapper, 403, "permission denied")))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/api/auth/login", "/api/auth/logout", "/api/auth/csrf")
                            .permitAll()
                        .requestMatchers("/api/timeseries/data/**", "/api/timeseries/statistics/**")
                            .hasAuthority(PermissionCode.HISTORY_DATA.name())
                        .requestMatchers(
                                "/api/timeseries/instances/**",
                                "/api/timeseries/semantic/**",
                                "/api/timeseries/window-config/**",
                                "/api/timeseries/derived-series/**",
                                "/api/timeseries/projects/**",
                                "/api/timeseries/cache/**")
                            .hasAuthority(PermissionCode.CONFIG_INFO.name())
                        .requestMatchers(
                                "/api/timeseries/anomaly-tasks/**",
                                "/api/timeseries/forecast-tasks/**",
                                "/api/timeseries/anomaly-results/**",
                                "/api/timeseries/forecast-results/**",
                                "/api/timeseries/events/**",
                                "/api/timeseries/decision/**")
                            .hasAuthority(PermissionCode.TASK_INFO.name())
                        .anyRequest().authenticated());
        return http.build();
    }

    private void writeError(
            jakarta.servlet.http.HttpServletResponse response,
            ObjectMapper objectMapper,
            int status,
            String message) throws java.io.IOException {
        response.setStatus(status);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType("application/json;charset=UTF-8");
        objectMapper.writeValue(response.getWriter(), ApiResult.fail(message));
    }
}

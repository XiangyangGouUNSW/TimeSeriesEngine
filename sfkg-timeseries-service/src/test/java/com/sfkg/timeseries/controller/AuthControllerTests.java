package com.sfkg.timeseries.controller;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sfkg.timeseries.auth.AuthenticatedUser;
import com.sfkg.timeseries.auth.LoginRequest;
import com.sfkg.timeseries.auth.PermissionCode;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class AuthControllerTests {

    @Test
    void successfulLoginWritesAuthenticationToSession() throws Exception {
        AuthenticationManager authenticationManager = mock(AuthenticationManager.class);
        AuthenticatedUser user = new AuthenticatedUser(
                "u001",
                "operator",
                Set.of(PermissionCode.HISTORY_DATA.name(), PermissionCode.CONFIG_INFO.name()));
        when(authenticationManager.authenticate(any(UsernamePasswordAuthenticationToken.class)))
                .thenReturn(new UsernamePasswordAuthenticationToken(user, null, user.getAuthorities()));

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new AuthController(
                authenticationManager,
                new HttpSessionSecurityContextRepository())).build();

        MvcResult result = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"operator\",\"password\":\"secret\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.userId").value("u001"))
                .andExpect(jsonPath("$.data.username").value("operator"))
                .andReturn();

        assertNotNull(result.getRequest().getSession(false));
        assertNotNull(result.getRequest().getSession(false).getAttribute(
                HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY));
    }
}

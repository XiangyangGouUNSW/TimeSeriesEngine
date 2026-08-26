package com.sfkg.timeseries.auth;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionUserAccount {

    private String userId;
    private String username;
    private String passwordHash;
    private Set<String> permissions = new LinkedHashSet<>();
    private boolean enabled = true;
}

package com.sfkg.timeseries.auth;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "timeseries.auth")
public class SessionAuthProperties {

    private String userFile = "data/timeseries-users.json";
    private String bootstrapUsername = "";
    private String bootstrapPassword = "";
    private boolean useAuthApi;
    private String authApiUrl = "http://127.0.0.1:8081";
    private int authApiTimeoutMillis = 5000;
    private String rolePermissionFile = "classpath:role-permission-mappings.json";

    public String getUserFile() {
        return userFile;
    }

    public void setUserFile(String userFile) {
        this.userFile = userFile;
    }

    public String getBootstrapUsername() {
        return bootstrapUsername;
    }

    public void setBootstrapUsername(String bootstrapUsername) {
        this.bootstrapUsername = bootstrapUsername;
    }

    public String getBootstrapPassword() {
        return bootstrapPassword;
    }

    public void setBootstrapPassword(String bootstrapPassword) {
        this.bootstrapPassword = bootstrapPassword;
    }

    public boolean isUseAuthApi() {
        return useAuthApi;
    }

    public void setUseAuthApi(boolean useAuthApi) {
        this.useAuthApi = useAuthApi;
    }

    public String getAuthApiUrl() {
        return authApiUrl;
    }

    public void setAuthApiUrl(String authApiUrl) {
        this.authApiUrl = authApiUrl;
    }

    public int getAuthApiTimeoutMillis() {
        return authApiTimeoutMillis;
    }

    public void setAuthApiTimeoutMillis(int authApiTimeoutMillis) {
        this.authApiTimeoutMillis = authApiTimeoutMillis;
    }

    public String getRolePermissionFile() {
        return rolePermissionFile;
    }

    public void setRolePermissionFile(String rolePermissionFile) {
        this.rolePermissionFile = rolePermissionFile;
    }
}

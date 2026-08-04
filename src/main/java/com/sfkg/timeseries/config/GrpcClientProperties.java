package com.sfkg.timeseries.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "timeseries.grpc")
public class GrpcClientProperties {

    private String coreAddress;
    private String anomalyAddress;
    private String forecastAddress;
    private String decisionAddress;
}

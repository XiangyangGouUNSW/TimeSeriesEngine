package com.sfkg.timeseries.config;

import com.sfkg.timeseries.grpc.server.EventReceiverGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcServerConfig.class);

    private final EventReceiverGrpcService eventReceiverService;
    private final int grpcServerPort;
    private Server server;

    public GrpcServerConfig(
            EventReceiverGrpcService eventReceiverService,
            @Value("${timeseries.grpc.server-port:9105}") int grpcServerPort) {
        this.eventReceiverService = eventReceiverService;
        this.grpcServerPort = grpcServerPort;
    }

    @jakarta.annotation.PostConstruct
    public void start() throws IOException {
        server = ServerBuilder.forPort(grpcServerPort)
                .addService(eventReceiverService)
                .build()
                .start();
        LOG.info("gRPC event receiver server started on port {}", grpcServerPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down gRPC server...");
            stop();
        }));
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
            LOG.info("gRPC server stopped");
        }
    }
}

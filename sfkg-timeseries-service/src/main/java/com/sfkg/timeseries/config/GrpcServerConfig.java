package com.sfkg.timeseries.config;

import com.sfkg.timeseries.grpc.server.AnomalyResultReceiverGrpcService;
import com.sfkg.timeseries.grpc.server.ConstraintResultReceiverGrpcService;
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
    private final AnomalyResultReceiverGrpcService anomalyResultReceiverService;
    private final ConstraintResultReceiverGrpcService constraintResultReceiverService;
    private final int grpcServerPort;
    private final int analysisReceiverPort;
    private Server server;
    private Server analysisServer;

    public GrpcServerConfig(
            EventReceiverGrpcService eventReceiverService,
            AnomalyResultReceiverGrpcService anomalyResultReceiverService,
            ConstraintResultReceiverGrpcService constraintResultReceiverService,
            @Value("${timeseries.grpc.server-port:9105}") int grpcServerPort,
            @Value("${timeseries.grpc.analysis-receiver-port:9106}") int analysisReceiverPort) {
        this.eventReceiverService = eventReceiverService;
        this.anomalyResultReceiverService = anomalyResultReceiverService;
        this.constraintResultReceiverService = constraintResultReceiverService;
        this.grpcServerPort = grpcServerPort;
        this.analysisReceiverPort = analysisReceiverPort;
    }

    @jakarta.annotation.PostConstruct
    public void start() throws IOException {
        server = ServerBuilder.forPort(grpcServerPort)
                .addService(eventReceiverService)
                .addService(constraintResultReceiverService)
                .build()
                .start();
        LOG.info("gRPC receiver server started on port {} (Event + Constraint)", grpcServerPort);

        analysisServer = ServerBuilder.forPort(analysisReceiverPort)
                .addService(anomalyResultReceiverService)
                .build()
                .start();
        LOG.info("gRPC analysis result receiver server started on port {}", analysisReceiverPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down gRPC servers...");
            stop();
        }));
    }

    @PreDestroy
    public void stop() {
        shutdownServer(server, "event-receiver");
        shutdownServer(analysisServer, "analysis-receiver");
    }

    private void shutdownServer(Server srv, String name) {
        if (srv != null) {
            try {
                srv.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                srv.shutdownNow();
            }
            LOG.info("gRPC {} server stopped", name);
        }
    }
}

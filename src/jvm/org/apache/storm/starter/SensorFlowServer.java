package org.apache.storm.starter;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class SensorFlowServer {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowServer.class);
    private final boolean debug;
    private final int port;
    private Server server;
    private SensorFlowCloudImpl service;
    private final double latencyMs;
    private final double bandwidthKbps;

    SensorFlowServer(int port, boolean debug, double latencyMs, double bandwidthKbps) {
        this.debug = debug;
        this.port = port;
        this.latencyMs = latencyMs;
        this.bandwidthKbps = bandwidthKbps;
    }

    void start() throws IOException {
        // register a runtime hook to shut down the server
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SensorFlowServer.this.stop();
            }
        });

        log.info("Listening on port {}", port);
        service = new SensorFlowCloudImpl(debug, latencyMs, bandwidthKbps);
        server = ServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();
    }

    private void stop() {
        log.info("Requesting server shutdown...");
        if (service != null) {
            service.shutdown();
        }
        if (server != null) {
            server.shutdown();
            log.info("Server has shut down.");
        }
    }

    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}

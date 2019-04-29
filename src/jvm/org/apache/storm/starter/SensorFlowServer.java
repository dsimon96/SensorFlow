package org.apache.storm.starter;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class SensorFlowServer {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowServer.class);
    private boolean debug;
    private String edgeHost;
    private int port;
    private Server server;

    SensorFlowServer(String edgeHost, int port, boolean debug) {
        this.debug = debug;
        this.edgeHost = edgeHost;
        this.port = port;
    }

    void start() throws IOException {
        // register a runtime hook to shut down the server
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SensorFlowServer.this.stop();
            }
        });

        server = ServerBuilder.forPort(port)
                .addService(new SensorFlowCloudImpl(debug))
                .build()
                .start();
    }

    private void stop() {
        log.info("Requesting server shutdown...");
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

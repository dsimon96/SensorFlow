package org.apache.storm.starter;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

class SensorFlowServer {
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
        server = ServerBuilder.forPort(port)
                .addService(new SensorFlowCloudImpl())
                .build()
                .start();

        // register a runtime hook to shut down the server
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SensorFlowServer.this.stop();
            }
        });
    }

    private void stop() {
        System.out.println("Requesting server shutdown...");
        if (server != null) {
            server.shutdown();
            System.out.println("Server has shut down.");
        }
    }

    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}

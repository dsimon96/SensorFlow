package org.apache.storm.starter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.storm.starter.proto.Empty;
import org.apache.storm.starter.proto.SensorFlowCloudGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class SensorFlowClient {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowClient.class);
    private final ManagedChannel channel;
    private final SensorFlowCloudGrpc.SensorFlowCloudBlockingStub stub;
    private final ExecutionManager manager;

    SensorFlowClient(String host, int port, boolean debug) {
        manager = new ExecutionManager(false, debug);

        log.info("Connecting to {}:{}", host, port);
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        stub = SensorFlowCloudGrpc.newBlockingStub(channel);
    }

    void start() {
        log.info("Submitting job");
        String token = stub.submitJob(Empty.newBuilder().build()).getToken();
        log.info("Client got token {}", token);
        manager.addJob(token);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        manager.shutdown();
    }

    void blockUntilShutdown() {
        try {
            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

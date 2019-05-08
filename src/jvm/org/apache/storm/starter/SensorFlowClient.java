package org.apache.storm.starter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.storm.starter.proto.DeletionReply;
import org.apache.storm.starter.proto.Empty;
import org.apache.storm.starter.proto.JobToken;
import org.apache.storm.starter.proto.SensorFlowCloudGrpc;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class SensorFlowClient {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowClient.class);
    private final ManagedChannel channel;
    private final SensorFlowCloudGrpc.SensorFlowCloudBlockingStub stub;
    private final ExecutionManager manager;
    private String token = null;

    SensorFlowClient(String host, int port, boolean debug, double latencyMs, double bandwidthKbps) {
        manager = new ExecutionManager(false, debug, latencyMs, bandwidthKbps);

        log.info("Connecting to {}:{}", host, port);
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        stub = SensorFlowCloudGrpc.newBlockingStub(channel);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SensorFlowClient.this.shutdown();
            }
        });
    }

    void start() {
        log.info("Submitting job");
        token = stub.submitJob(Empty.newBuilder().build()).getToken();
        log.info("Client got token {}", token);
        manager.addJob(token);

        while (true) {
            Utils.sleep(20000);
            manager.rescheduleAll(stub);
        }
    }

    public void shutdown() {
        if (token != null) {
            manager.deleteJob(token);
            DeletionReply reply = stub.deleteJob(JobToken.newBuilder().setToken(token).build());
            if (reply.getSuccess()) {
                log.info("Successfully deleted job on cloud.");
            } else {
                log.info("Failed to delete job on cloud.");
            }
        }
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            manager.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

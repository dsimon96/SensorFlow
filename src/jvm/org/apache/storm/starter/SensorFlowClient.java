package org.apache.storm.starter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.storm.starter.proto.DeletionReply;
import org.apache.storm.starter.proto.Empty;
import org.apache.storm.starter.proto.JobToken;
import org.apache.storm.starter.proto.SensorFlowCloudGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class SensorFlowClient {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowClient.class);
    private boolean debug;
    private final ManagedChannel channel;
    private final SensorFlowCloudGrpc.SensorFlowCloudBlockingStub stub;
    private final ExecutionManager manager;

    SensorFlowClient(String host, int port, boolean debug) {
        this.debug = debug;
        manager = new ExecutionManager(false, debug);

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

        manager.deleteJob(token);
        DeletionReply reply = stub.deleteJob(JobToken.newBuilder().setToken(token).build());
        if (reply.getSuccess()) {
            log.info("Successfully deleted job");
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    void blockUntilShutdown() {
    }
}

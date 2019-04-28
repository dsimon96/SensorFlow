package org.apache.storm.starter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.storm.starter.proto.JobToken;
import org.apache.storm.starter.proto.SensorFlowCloudGrpc;
import org.apache.storm.starter.proto.StatusReply;

import java.util.concurrent.TimeUnit;

class SensorFlowClient {
    private String host;
    private int port;
    private boolean debug;
    private final ManagedChannel channel;
    private final SensorFlowCloudGrpc.SensorFlowCloudBlockingStub stub;

    SensorFlowClient(String host, int port, boolean debug) {
        this.host = host;
        this.port = port;
        this.debug = debug;

        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        stub = SensorFlowCloudGrpc.newBlockingStub(channel);
    }

    void start() {
        StatusReply reply = stub.getJobStatus(JobToken.newBuilder().build());
        StatusReply.Status status = StatusReply.Status.forNumber(reply.getStatusValue());
        System.out.println(status.getValueDescriptor());
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    void blockUntilShutdown() {
    }
}

package org.apache.storm.starter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.storm.starter.proto.*;

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
        StatusReply reply = stub.getJobStatus(JobToken.newBuilder().setToken("").build());
        StatusReply.Status status = StatusReply.Status.forNumber(reply.getStatusValue());
        System.out.println(status.getValueDescriptor());

        String token = stub.submitJob(Empty.newBuilder().build()).getToken();
        System.out.println("Got job token ".concat(token));

        reply = stub.getJobStatus(JobToken.newBuilder().setToken(token).build());
        status = StatusReply.Status.forNumber(reply.getStatusValue());
        System.out.println(status.getValueDescriptor());

        DeletionReply reply1 = stub.deleteJob(JobToken.newBuilder().setToken(token).build());
        if (reply1.getSuccess()) {
            System.out.println("Deleted job");
        } else {
            System.out.println("Failed to delete job");
        }

        reply = stub.getJobStatus(JobToken.newBuilder().setToken(token).build());
        status = StatusReply.Status.forNumber(reply.getStatusValue());
        System.out.println(status.getValueDescriptor());
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    void blockUntilShutdown() {
    }
}

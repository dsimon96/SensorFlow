package org.apache.storm.starter;

import io.grpc.stub.StreamObserver;
import org.apache.storm.starter.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SensorFlowCloudImpl extends SensorFlowCloudGrpc.SensorFlowCloudImplBase {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowCloudImpl.class);
    private final ExecutionManager manager;

    SensorFlowCloudImpl(boolean debug) {
        manager = new ExecutionManager(true, debug);
    }

    @Override
    public void submitJob(Empty request, StreamObserver<JobToken> responseObserver) {
        log.info("SubmitJob()");
        String token = manager.newJob();
        JobToken reply = JobToken.newBuilder()
                .setToken(token)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getJobStatus(JobToken request, StreamObserver<StatusReply> responseObserver) {
        String token = request.getToken();
        log.info("GetJobStatus({})", token);
        StatusReply reply = StatusReply.newBuilder()
                .setStatus(manager.getJobStatus(token))
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteJob(JobToken request, StreamObserver<DeletionReply> responseObserver) {
        String token = request.getToken();
        log.info("DeleteJob({})", token);
        DeletionReply reply = DeletionReply.newBuilder()
                .setSuccess(manager.deleteJob(token))
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void setJobSchedule(JobSchedule request, StreamObserver<ScheduleReply> responseObserver) {
        super.setJobSchedule(request, responseObserver);
    }

    void shutdown() {
        manager.shutdown();
    }
}

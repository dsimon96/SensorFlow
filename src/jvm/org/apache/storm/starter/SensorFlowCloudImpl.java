package org.apache.storm.starter;

import io.grpc.stub.StreamObserver;
import org.apache.storm.starter.proto.*;

import java.util.HashMap;
import java.util.UUID;

public class SensorFlowCloudImpl extends SensorFlowCloudGrpc.SensorFlowCloudImplBase {
    private HashMap<String, SensorFlowJob> jobs = new HashMap<>();

    SensorFlowCloudImpl() {
    }

    @Override
    public void submitJob(Empty request, StreamObserver<JobToken> responseObserver) {
        String token = UUID.randomUUID().toString();
        SensorFlowJob job = new SensorFlowJob();
        job.start();
        JobToken reply = JobToken.newBuilder().setToken(token).build();
        jobs.put(token, job);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getJobStatus(JobToken request, StreamObserver<StatusReply> responseObserver) {
        String token = request.getToken();
        StatusReply.Builder replyBuilder = StatusReply.newBuilder();
        if (jobs.containsKey(token)) {
            replyBuilder.setStatus(jobs.get(token).getStatus());
        } else {
            replyBuilder.setStatus(StatusReply.Status.DoesNotExist);
        }
        responseObserver.onNext(replyBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteJob(JobToken request, StreamObserver<DeletionReply> responseObserver) {
        String token = request.getToken();
        boolean success;
        if (jobs.containsKey(token)) {
            SensorFlowJob job = jobs.get(token);
            job.stop();
            success = true;
        } else {
            success = false;
        }
        DeletionReply reply = DeletionReply.newBuilder().setSuccess(success).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void setJobSchedule(JobSchedule request, StreamObserver<ScheduleReply> responseObserver) {
        super.setJobSchedule(request, responseObserver);
    }
}

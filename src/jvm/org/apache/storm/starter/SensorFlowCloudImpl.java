package org.apache.storm.starter;

import io.grpc.stub.StreamObserver;
import org.apache.storm.starter.proto.*;

public class SensorFlowCloudImpl extends SensorFlowCloudGrpc.SensorFlowCloudImplBase {
    SensorFlowCloudImpl() {

    }

    @Override
    public void submitJob(Empty request, StreamObserver<JobToken> responseObserver) {
        super.submitJob(request, responseObserver);
    }

    @Override
    public void getJobStatus(JobToken request, StreamObserver<StatusReply> responseObserver) {
        StatusReply reply = StatusReply.newBuilder().setStatus(StatusReply.Status.DoesNotExist).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteJob(JobToken request, StreamObserver<DeletionReply> responseObserver) {
        super.deleteJob(request, responseObserver);
    }

    @Override
    public void setJobSchedule(JobSchedule request, StreamObserver<ScheduleReply> responseObserver) {
        super.setJobSchedule(request, responseObserver);
    }
}

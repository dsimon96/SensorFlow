package org.apache.storm.starter;

import org.apache.storm.LocalCluster;
import org.apache.storm.starter.proto.JobSchedule;
import org.apache.storm.starter.proto.ScheduleReply;
import org.apache.storm.starter.proto.SensorFlowCloudGrpc;
import org.apache.storm.starter.proto.StatusReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

class ExecutionManager {
    private final static Logger log = LoggerFactory.getLogger(ExecutionManager.class);
    private final LocalCluster cluster;
    private final ConcurrentMap<String, SensorFlowJob> jobs = new ConcurrentHashMap<>();
    private final boolean isCloud;
    private final boolean debug;
    private final double latencyMs;
    private final double bandwidthKbps;

    ExecutionManager(boolean isCloud, boolean debug, double latencyMs, double bandwidthKbps) {
        this.isCloud = isCloud;
        this.debug = debug;
        this.latencyMs = latencyMs;
        this.bandwidthKbps = bandwidthKbps;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ExecutionManager.this.shutdown();
            }
        });
        cluster = new LocalCluster();
    }

    void shutdown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    private String getNewToken() {
        if (debug) {
            return "debug";
        } else {
            return "myapp"; //UUID.randomUUID().toString();
        }
    }

    String newJob() {
        log.info("Creating new job");
        String token = getNewToken();
        addJob(token);
        return token;
    }

    void addJob(String token) {
        log.info("Adding new job with token {}", token);
        SensorFlowJob job = new SensorFlowJob(isCloud, debug, token, cluster, latencyMs, bandwidthKbps);
        jobs.put(token, job);
        job.start();
    }

    boolean deleteJob(String token) {
        SensorFlowJob job = jobs.remove(token);
        if (job != null) {
            job.stop();
            return true;
        } else {
            return false;
        }
    }

    StatusReply.Status getJobStatus(String token) {
        SensorFlowJob job = jobs.get(token);
        if (job == null) {
            return StatusReply.Status.DoesNotExist;
        } else {
            return job.getStatus();
        }
    }

    void rescheduleAll(SensorFlowCloudGrpc.SensorFlowCloudBlockingStub stub) {
        for (SensorFlowJob job : jobs.values()) {
            String token = job.getToken();

            Map<String, Boolean> sched = new HashMap<>();
            sched.put("clap1", ThreadLocalRandom.current().nextBoolean());
            sched.put("clap2", ThreadLocalRandom.current().nextBoolean());

            ScheduleReply reply = stub.setJobSchedule(JobSchedule.newBuilder()
                    .setToken(token)
                    .putAllSchedule(sched)
                    .build());

            if (reply.getSuccess() && setJobSchedule(token, sched)) {
                log.info("Successfully set schedule for job {}.", token);
            } else {
                log.info("Failed to reschedule job {}.", token);
            }
        }
    }

    boolean setJobSchedule(String token, Map<String, Boolean> scheduleMap) {
        SensorFlowJob job = jobs.get(token);
        if (job != null) {
            return job.setSchedule(scheduleMap);
        } else {
            return false;
        }
    }
}

package org.apache.storm.starter;

import org.apache.storm.LocalCluster;
import org.apache.storm.starter.proto.StatusReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class ExecutionManager {
    private final static Logger log = LoggerFactory.getLogger(ExecutionManager.class);
    private final LocalCluster cluster = new LocalCluster();
    private final ConcurrentMap<String, SensorFlowJob> jobs = new ConcurrentHashMap<>();
    private final boolean isCloud;
    private final boolean debug;

    ExecutionManager(boolean isCloud, boolean debug) {
        this.isCloud = isCloud;
        this.debug = debug;
    }

    String newJob() {
        log.info("Creating new job");
        String token = UUID.randomUUID().toString();
        addJob(token);
        return token;
    }

    void addJob(String token) {
        log.info("Adding new job with token {}", token);
        SensorFlowJob job = new SensorFlowJob(isCloud, debug, token);
        jobs.put(token, job);
        job.start(cluster);
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
}

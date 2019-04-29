package org.apache.storm.starter;

import org.apache.storm.LocalCluster;
import org.apache.storm.starter.proto.StatusReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SensorFlowJob {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowJob.class);
    private String token;
    private boolean isCloud;
    private boolean debug;
    private boolean isInitialized = false;
    private boolean isRunning = false;

    SensorFlowJob(boolean isCloud, boolean debug, String token) {
        this.isCloud = isCloud;
        this.token = token;
    }

    void start(LocalCluster cluster) {
        log.info("Starting job {}", token);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SensorFlowJob.this.stop();
            }
        });

        isInitialized = true;
        isRunning = true;
    }

    void stop() {
        log.info("Stopping job {}", token);
        if (isRunning) {
            isRunning = false;
        }
    }

    String getToken() {
        return token;
    }

    StatusReply.Status getStatus() {
        if (!isInitialized) {
            return StatusReply.Status.Creating;
        } else if (isRunning) {
            return StatusReply.Status.Running;
        } else {
            return StatusReply.Status.Done;
        }
    }
}

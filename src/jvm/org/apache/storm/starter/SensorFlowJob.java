package org.apache.storm.starter;

import org.apache.storm.starter.proto.StatusReply;

class SensorFlowJob {
    private boolean isInitialized = false;
    private boolean isRunning;

    void start() {
        isInitialized = true;
        isRunning = true;
    }

    void stop() {
        isRunning = false;
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

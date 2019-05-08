package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.proto.StatusReply;
import org.apache.storm.starter.sfgraph.LogicalGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class SensorFlowJob {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowJob.class);
    private final String token;
    private final boolean isCloud;
    private final boolean debug;
    private final LocalCluster cluster;
    private boolean isInitialized = false;
    private boolean isRunning = false;
    private final double latencyMs;
    private final double bandwidthKbps;
    private LogicalGraph graph;

    SensorFlowJob(boolean isCloud, boolean debug, String token, LocalCluster cluster, double latencyMs, double bandwidthKbps) {
        this.isCloud = isCloud;
        this.debug = debug;
        this.token = token;
        this.cluster = cluster;
        this.latencyMs = latencyMs;
        this.bandwidthKbps = bandwidthKbps;
    }

    void start() {
        log.info("Starting job {}", token);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                SensorFlowJob.this.stop();
            }
        });

        isInitialized = true;
        isRunning = true;

        Config conf = new Config();
        conf.setDebug(debug);

        graph = ClapDetectionTopologyAllOnOne.CreateLogicalGraph(token, isCloud, latencyMs, bandwidthKbps);

        StormTopology topology = ClapDetectionTopologyAllOnOne.CreateClapDetectionTopologyAllOnOne(isCloud, token, debug, false);

        cluster.submitTopology(token, conf, topology);
    }

    void stop() {
        log.info("Stopping job {}", token);
        if (isRunning) {
            cluster.killTopology(token);
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

    boolean setSchedule(Map<String, Boolean> schedule) {
        return graph.setSchedule(schedule);
    }
}

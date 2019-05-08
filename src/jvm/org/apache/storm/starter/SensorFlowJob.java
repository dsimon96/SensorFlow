package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.proto.StatusReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

class SensorFlowJob {
    private final static Logger log = LoggerFactory.getLogger(SensorFlowJob.class);
    private final String token;
    private final boolean isCloud;
    private final boolean debug;
    private final LocalCluster cluster;
    private boolean isInitialized = false;
    private boolean isRunning = false;

    SensorFlowJob(boolean isCloud, boolean debug, String token, LocalCluster cluster) {
        this.isCloud = isCloud;
        this.debug = debug;
        this.token = token;
        this.cluster = cluster;
    }

    public static void WriteToSplitterFile(String splitterBoltId, String content) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("src/" + splitterBoltId + ".txt"));
            writer.write(content);
            writer.close();
        }
        catch (Exception e) {
            System.out.println("Error when writing to file: " + e.toString());
        }
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

        StormTopology topology = ClapDetectionTopologyAllOnOne.CreateClapDetectionTopologyAllOnOne(isCloud, token, debug, isCloud);

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
}

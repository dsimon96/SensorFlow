package org.apache.storm.starter.sfgraph;

import org.apache.storm.starter.SensorFlowBenchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class LogicalGraph {
    private final static Logger log = LoggerFactory.getLogger(LogicalGraph.class);
    final SensorSource source;
    final ActuationSink sink;
    Map<String, String> bolts = new HashMap<>(); // Map data bolt to splitter bolt.

    LogicalGraph(SensorSource s, ActuationSink t) {
        source = s;
        sink = t;
    }

    public boolean setSchedule(Map<String, Boolean> schedule) {
        for (String dataBoltId : schedule.keySet()) {
            if (!bolts.containsKey(dataBoltId)) {
                log.error("Bolt {} not found in graph", dataBoltId);
                return false;
            }
        }
        for (String dataBoltId : schedule.keySet()) {
            String splitterBoltId = bolts.get(dataBoltId);
            Boolean cloud = schedule.get(dataBoltId);
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter("src/" + splitterBoltId + ".txt"));
                if (cloud == null) {
                    writer.write("random");
                } else if (cloud) {
                    writer.write("cloud");
                }
                else { writer.write("edge"); }
                writer.close();
            } catch (Exception e) {
                System.out.println("Error when writing to file: " + e.toString());
                return false;
            }
        }
        return true;
    }

    public Map<String, Boolean> getOptSchedule(Map<String, Double> remoteCosts) {
        Map<String, Boolean> res = new HashMap<>();
        for (String name : bolts.keySet()) {
            res.put(name, false);
        }

        BestPath init = source.getBestPath(remoteCosts, 0);
        for (BestPath path = init; path != null; path = path.getNext()) {
            SFNode node = path.getNode();
            String boltName = node.getBoltName();
            if (boltName != null) {
                Boolean isCloud = node.isCloud();
                res.put(boltName, isCloud);
            }
        }

        return res;
    }

    Map<String, Double> getNodeLatencies() {
        Map<String, Double> res = new HashMap<>();

        for (String name : bolts.keySet()) {
            res.put(name, SensorFlowBenchmark.get(name));
        }

        return res;
    }

    public boolean resetSchedule() {
        Map<String, Boolean> sched = new HashMap<>();
        for (String bolt : bolts.keySet()) {
            sched.put(bolt, null);
        }

        return setSchedule(sched);
    }

    public Map<String, Double> getBoltLatencies() {
        Map<String, Double> latencies = new HashMap<>();
        for (String bolt : bolts.keySet()) {
            latencies.put(bolt, SensorFlowBenchmark.get(bolt));
        }
        return latencies;
    }
}

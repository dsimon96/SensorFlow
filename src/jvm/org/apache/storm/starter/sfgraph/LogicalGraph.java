package org.apache.storm.starter.sfgraph;

import org.apache.storm.starter.SensorFlowBenchmark;

import java.util.HashMap;
import java.util.Map;

public class LogicalGraph {
    final SensorSource source;
    final ActuationSink sink;
    Map<String, String> bolts = new HashMap<>();

    LogicalGraph(SensorSource s, ActuationSink t) {
        source = s;
        sink = t;
    }

    Map<String, Boolean> getOptSchedule(Map<String, Double> remoteCosts) {
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
}

package org.apache.storm.starter.sfgraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LogicalGraph {
    final SensorSource source;
    final ActuationSink sink;
    Set<String> boltNames = new HashSet<>();

    LogicalGraph(SensorSource s, ActuationSink t) {
        source = s;
        sink = t;
    }

    Map<String, Boolean> getOptSchedule(Map<String, Double> remoteCosts) {
        Map<String, Boolean> res = new HashMap<>();
        for (String name : boltNames) {
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
}

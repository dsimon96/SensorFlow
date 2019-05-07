package org.apache.storm.starter.sfgraph;

class BestPath {
    private double latency;
    private SFNode node;
    private BestPath next;

    BestPath(double latency, SFNode node, BestPath next) {
        this.latency = latency;
        this.node = node;
        this.next = next;
    }

    double getLatency() {
        return latency;
    }

    BestPath getNext() {
        return next;
    }

    SFNode getNode() {
        return node;
    }
}

package org.apache.storm.starter.sfgraph;

import java.util.Map;

public class BoltEgressNode implements SFNode {
    private final double localLatencyMs;
    private final double wanLatencyMs;
    private final double wanThroughputKbps;
    private final boolean isCloud;
    private SFNode edgeNext;
    private SFNode cloudNext;
    private double sizeMultiplier = 1.0;
    private double sizeAdded = 0.0;

    BoltEgressNode(boolean isCloud, double localLatencyMs, double wanLatencyMs, double wanThroughputKbps) {
        this.isCloud = isCloud;
        this.localLatencyMs = localLatencyMs;
        this.wanLatencyMs = wanLatencyMs;
        this.wanThroughputKbps = wanThroughputKbps;
    }

    @Override
    public BestPath getBestPath(Map<String, Double> remoteCosts, double inputSize) {
        double outputSize = getOutputSize(inputSize);

        BestPath edgePath = edgeNext.getBestPath(remoteCosts, outputSize);
        double edgeLatency;
        if (cloudNext == null) {
            if (isCloud) {
                edgeLatency = wanLatencyMs + (outputSize / wanThroughputKbps) + edgePath.getLatency();
            } else {
                edgeLatency = localLatencyMs + edgePath.getLatency();
            }
            return new BestPath(edgeLatency, this, edgePath);
        }

        BestPath cloudPath = cloudNext.getBestPath(remoteCosts, outputSize);
        double cloudLatency;
        if (isCloud) {
            edgeLatency = wanLatencyMs + (outputSize / wanThroughputKbps) + edgePath.getLatency();
            cloudLatency = localLatencyMs + cloudPath.getLatency();
        } else {
            edgeLatency = localLatencyMs + edgePath.getLatency();
            cloudLatency = wanLatencyMs + (outputSize / wanThroughputKbps) + cloudPath.getLatency();
        }

        if (edgeLatency <= cloudLatency) {
            return new BestPath(edgeLatency, this, edgePath);
        } else {
            return new BestPath(cloudLatency, this, cloudPath);
        }
    }

    @Override
    public void setEdgeNext(SFNode node) {
        edgeNext = node;
    }

    @Override
    public void setCloudNext(SFNode node) {
        cloudNext = node;
    }

    @Override
    public void setNext(SFNode node) {
        edgeNext = node;
        cloudNext = null;
    }

    @Override
    public void setDataSize(double dataSize) {
        sizeMultiplier = 0.0;
        sizeAdded = dataSize;
    }

    @Override
    public void multiplyDataSize(double multiplier) {
        sizeMultiplier *= multiplier;
        sizeAdded *= multiplier;
    }

    @Override
    public void addToDataSize(double size) {
        sizeAdded += size;
    }

    @Override
    public double getOutputSize() {
        return getOutputSize(0);
    }

    @Override
    public double getOutputSize(double inputSize) {
        return inputSize * sizeMultiplier + sizeAdded;
    }

    @Override
    public String getBoltName() {
        return null;
    }

    @Override
    public Boolean isCloud() {
        return null;
    }
}

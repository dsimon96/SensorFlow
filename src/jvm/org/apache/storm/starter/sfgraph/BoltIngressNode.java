package org.apache.storm.starter.sfgraph;

import org.apache.storm.starter.SensorFlowBenchmark;

import java.util.Map;

public class BoltIngressNode implements SFNode {
    private BoltEgressNode next;
    private boolean isCloud;
    private String name;
    private double sizeMultiplier = 1.0;
    private double sizeAdded = 0.0;

    BoltIngressNode(boolean isCloud, String name) {
        this.isCloud = isCloud;
        this.name = name;
    }

    @Override
    public BestPath getBestPath(Map<String, Double> remoteCosts, double inputSize) {
        double compLatency;
        if (isCloud) {
            compLatency = remoteCosts.get(name);
        } else {
            compLatency = SensorFlowBenchmark.get(name);
        }
        BestPath nextPath = next.getBestPath(remoteCosts, getOutputSize(inputSize));
        return new BestPath(compLatency + nextPath.getLatency(), this, nextPath);
    }

    @Override
    public void setEdgeNext(SFNode node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCloudNext(SFNode node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNext(SFNode node) {
        next = (BoltEgressNode) node;
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
        return name;
    }

    @Override
    public Boolean isCloud() {
        return isCloud;
    }
}

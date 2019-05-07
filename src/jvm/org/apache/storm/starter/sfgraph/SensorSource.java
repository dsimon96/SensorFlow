package org.apache.storm.starter.sfgraph;

import java.util.Map;

class SensorSource implements SFNode {
    private final double localLatencyMs;
    private final double wanLatencyMs;
    private final double wanBandwidthKbps;

    private SFNode edgeNext;
    private SFNode cloudNext;

    private double dataSize = 1.0;

    SensorSource(double localLatencyMs, double wanLatencyMs, double wanBandwidthKbps) {
        this.localLatencyMs = localLatencyMs;
        this.wanLatencyMs = wanLatencyMs;
        this.wanBandwidthKbps = wanBandwidthKbps;
    }

    @Override
    public BestPath getBestPath(Map<String, Double> remoteCosts, double inputSize) {
        BestPath localNext = edgeNext.getBestPath(remoteCosts, dataSize);
        BestPath remoteNext = cloudNext.getBestPath(remoteCosts, dataSize);

        double latencyLocal = localLatencyMs + localNext.getLatency();
        double latencyRemote = wanLatencyMs + (dataSize / wanBandwidthKbps) + remoteNext.getLatency();

        if (latencyLocal <= latencyRemote) {
            return new BestPath(latencyLocal, this, localNext);
        } else {
            return new BestPath(latencyRemote, this, remoteNext);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDataSize(double dataSize) {
        this.dataSize = dataSize;
    }

    @Override
    public void multiplyDataSize(double multiplier) {
        this.dataSize *= multiplier;
    }

    @Override
    public void addToDataSize(double size) {
        this.dataSize += size;
    }

    @Override
    public double getOutputSize() {
        return dataSize;
    }

    @Override
    public double getOutputSize(double inputSize) {
        return getOutputSize();
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

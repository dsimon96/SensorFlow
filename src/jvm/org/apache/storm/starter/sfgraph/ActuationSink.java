package org.apache.storm.starter.sfgraph;

import java.util.Map;

public class ActuationSink implements SFNode {
    @Override
    public BestPath getBestPath(Map<String, Double> remoteCosts, double inputSize) {
        return new BestPath(0, this, null);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDataSize(double dataSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void multiplyDataSize(double multiplier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addToDataSize(double size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getOutputSize() {
        return 0;
    }

    @Override
    public double getOutputSize(double inputSize) {
        return 0;
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

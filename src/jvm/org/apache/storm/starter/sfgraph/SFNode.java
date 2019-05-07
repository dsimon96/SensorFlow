package org.apache.storm.starter.sfgraph;

import java.util.Map;

public interface SFNode {
    BestPath getBestPath(Map<String, Double> remoteCosts, double inputSize);

    void setEdgeNext(SFNode node);

    void setCloudNext(SFNode node);

    void setNext(SFNode node);

    void setDataSize(double dataSize);

    void multiplyDataSize(double multiplier);

    void addToDataSize(double size);

    double getOutputSize();

    double getOutputSize(double inputSize);

    String getBoltName();

    Boolean isCloud();
}

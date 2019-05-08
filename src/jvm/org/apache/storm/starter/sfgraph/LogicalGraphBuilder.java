package org.apache.storm.starter.sfgraph;

public class LogicalGraphBuilder {
    private final double localLatencyMs;
    private final double wanLatencyMs;
    private final double wanBandwidthKbps;
    private LogicalGraph graph;
    private SFNode cursorEdge;
    private SFNode cursorCloud;

    public LogicalGraphBuilder(double localLatencyMs, double wanLatencyMs, double wanBandwidthKbps) {
        this.localLatencyMs = localLatencyMs;
        this.wanLatencyMs = wanLatencyMs;
        this.wanBandwidthKbps = wanBandwidthKbps;

        graph = new LogicalGraph(new SensorSource(localLatencyMs, wanLatencyMs, wanBandwidthKbps),
                new ActuationSink());

        cursorCloud = graph.source;
        cursorEdge = graph.source;
    }

    public LogicalGraphBuilder addBolt(String name, String splitter) {
        graph.bolts.put(name, splitter);

        BoltIngressNode localIngress = new BoltIngressNode(false, name);
        BoltEgressNode localEgress = new BoltEgressNode(false, localLatencyMs, wanLatencyMs, wanBandwidthKbps);
        localIngress.setNext(localEgress);

        BoltIngressNode remoteIngress = new BoltIngressNode(true, name);
        BoltEgressNode remoteEgress = new BoltEgressNode(true, localLatencyMs, wanLatencyMs, wanBandwidthKbps);
        remoteIngress.setNext(remoteEgress);

        cursorCloud.setEdgeNext(localIngress);
        cursorCloud.setCloudNext(remoteEgress);

        if (cursorEdge != cursorCloud) {
            cursorEdge.setEdgeNext(localIngress);
            cursorEdge.setCloudNext(remoteIngress);
        }

        cursorCloud = localEgress;
        cursorEdge = remoteEgress;

        return this;
    }

    public LogicalGraphBuilder setDataSize(double size) {
        cursorCloud.setDataSize(size);
        cursorEdge.setDataSize(size);

        return this;
    }

    public LogicalGraphBuilder multiplyDataSize(double multiplier) {
        cursorCloud.multiplyDataSize(multiplier);
        if (cursorEdge != cursorCloud) {
            cursorEdge.multiplyDataSize(multiplier);
        }

        return this;
    }

    public LogicalGraphBuilder addToDataSize(double size) {
        cursorCloud.addToDataSize(size);
        if (cursorEdge != cursorCloud) {
            cursorEdge.addToDataSize(size);
        }

        return this;
    }

    public LogicalGraph build() {
        cursorCloud.setNext(graph.sink);
        cursorEdge.setNext(graph.sink);
        return graph;
    }
}


package org.apache.storm.starter.sfgraph;

public class LogicalGraphBuilder {
    private final double localLatencyMs;
    private final double wanLatencyMs;
    private final double wanBandwidthKbps;
    private LogicalGraph graph;
    private SFNode cursorRemote;
    private SFNode cursorLocal;

    public LogicalGraphBuilder(double localLatencyMs, double wanLatencyMs, double wanBandwidthKbps) {
        this.localLatencyMs = localLatencyMs;
        this.wanLatencyMs = wanLatencyMs;
        this.wanBandwidthKbps = wanBandwidthKbps;

        graph = new LogicalGraph(new SensorSource(localLatencyMs, wanLatencyMs, wanBandwidthKbps),
                new ActuationSink());

        cursorLocal = graph.source;
        cursorRemote = graph.source;
    }

    public LogicalGraphBuilder addBolt(String name) {
        graph.boltNames.add(name);

        BoltIngressNode localIngress = new BoltIngressNode(false, name);
        BoltEgressNode localEgress = new BoltEgressNode(false, localLatencyMs, wanLatencyMs, wanBandwidthKbps);
        localIngress.setNext(localEgress);

        BoltIngressNode remoteIngress = new BoltIngressNode(true, name);
        BoltEgressNode remoteEgress = new BoltEgressNode(true, localLatencyMs, wanLatencyMs, wanBandwidthKbps);
        remoteIngress.setNext(remoteEgress);

        cursorLocal.setEdgeNext(localIngress);
        cursorLocal.setCloudNext(remoteEgress);

        if (cursorRemote != cursorLocal) {
            cursorRemote.setEdgeNext(localIngress);
            cursorRemote.setCloudNext(remoteIngress);
        }

        cursorLocal = localEgress;
        cursorRemote = remoteEgress;

        return this;
    }

    public void setDataSize(double size) {
        cursorLocal.setDataSize(size);
        cursorRemote.setDataSize(size);
    }

    public void multiplyDataSize(double multiplier) {
        cursorLocal.multiplyDataSize(multiplier);
        if (cursorRemote != cursorLocal) {
            cursorRemote.multiplyDataSize(multiplier);
        }
    }

    public void addToDataSize(double size) {
        cursorLocal.addToDataSize(size);
        if (cursorRemote != cursorLocal) {
            cursorRemote.addToDataSize(size);
        }
    }

    public LogicalGraph build() {
        cursorLocal.setNext(graph.sink);
        cursorRemote.setNext(graph.sink);
        return graph;
    }
}


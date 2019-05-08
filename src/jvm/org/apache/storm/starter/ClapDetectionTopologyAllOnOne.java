package org.apache.storm.starter;

import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.TupleToMessage;
import io.latent.storm.rabbitmq.TupleToMessageNonDynamic;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ProducerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.sfgraph.LogicalGraph;
import org.apache.storm.starter.sfgraph.LogicalGraphBuilder;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

class ClapDetectionTopologyAllOnOne {

    // debug = false does not use a vhost.
    static StormTopology CreateClapDetectionTopologyAllOnOne(boolean cloud, String token, boolean debug, boolean sink_is_cloud) {
        TopologyBuilder builder = new TopologyBuilder();
        /* Begin RabbitMQ as Sensor Input */
        String suffix = "sensor-spout";
        IRichSpout sensorSpout = ClapDetectionTopology.CreateRabbitSpout(cloud, token, suffix); // Receiving sensor data, always edge.
        ConsumerConfig sensorSpoutConfig = ClapDetectionTopology.CreateRabbitSpoutConfig(cloud, debug); // Receiving sensor data, always edge.
        builder.setSpout("sensor-spout", sensorSpout, 1)
                .addConfigurations(sensorSpoutConfig.asMap())
                .setMaxSpoutPending(200);

        builder.setBolt("sensor-splitter", new ClapDetectionTopology.SplitterBolt(), 1).shuffleGrouping("sensor-spout");

        // Add sink to possibly route to cloud.
        TupleToMessage sensorSinkScheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) {
                System.out.println("Input from sensor spout was " + input.getString(0));
                return input.getString(0).getBytes();
            }
        };
        ProducerConfig sensorSinkCloudConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, "sensor-sink", debug, true);
        ProducerConfig sensorSinkEdgeConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, "sensor-sink", debug, false);

        builder.setBolt("sensor-sink-cloud", new RabbitMQBolt(sensorSinkScheme))
                .addConfigurations(sensorSinkCloudConfig.asMap())
                .shuffleGrouping("sensor-splitter", "cloud-stream");
        builder.setBolt("sensor-sink-edge", new RabbitMQBolt(sensorSinkScheme))
                .addConfigurations(sensorSinkEdgeConfig.asMap())
                .shuffleGrouping("sensor-splitter", "edge-stream");
        /* End RabbitMQ as Sensor Input */

        IRichSpout spout1 = ClapDetectionTopology.CreateRabbitSpout(cloud, token, "sensor-sink");
        ConsumerConfig spout1Config = ClapDetectionTopology.CreateRabbitSpoutConfig(cloud, debug);
        builder.setSpout("spout1", spout1, 1)
                .addConfigurations(spout1Config.asMap())
                .setMaxSpoutPending(200);


        builder.setBolt("clap1", new ClapDetectionTopology.ClapDetection1Bolt(), 1).shuffleGrouping("spout1");

        builder.setBolt("splitter1", new ClapDetectionTopology.SplitterBolt(), 1).shuffleGrouping("clap1");

        // Midbolt Sink and Spout
        TupleToMessage sink1Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) {
                return input.getStringByField("detection").getBytes();
            }
        };
        ProducerConfig sink1CloudConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, "sink1", debug, true);
        ProducerConfig sink1EdgeConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, "sink1", debug, false);

        builder.setBolt("sink1-cloud", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1CloudConfig.asMap())
                .shuffleGrouping("splitter1", "cloud-stream");
        builder.setBolt("sink1-edge", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1EdgeConfig.asMap())
                .shuffleGrouping("splitter1", "edge-stream");

        IRichSpout spout2 = ClapDetectionTopology.CreateRabbitSpout(cloud, token, "sink1");
        ConsumerConfig spout2Config = ClapDetectionTopology.CreateRabbitSpoutConfig(cloud, debug);
        builder.setSpout("spout2", spout2, 1)
                .addConfigurations(spout2Config.asMap())
                .setMaxSpoutPending(200);

        builder.setBolt("clap2", new ClapDetectionTopology.ClapDetection2Bolt(), 1).shuffleGrouping("spout2");

        // RabbitMQ as Sink, message attributes are non-dynamic
        TupleToMessage sink2Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) { return input.getStringByField("detection").getBytes(); }
        };

        // Rabbit Sink as Application Output
        suffix = "sink2";

        ProducerConfig sink2Config = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, suffix, debug, sink_is_cloud);

        builder.setBolt("sink2", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2Config.asMap())
                .shuffleGrouping("clap2");

        return builder.createTopology();
    }

    static LogicalGraph CreateLogicalGraph(String token, boolean isCloud, double latencyMs, double bandwidthKbps) {
        return new LogicalGraphBuilder(0, latencyMs, bandwidthKbps)
                .setDataSize(3.0)
                .addBolt("clap1", "sensor-splitter")
                .addBolt("clap2", "splitter1")
                .build();
    }
}

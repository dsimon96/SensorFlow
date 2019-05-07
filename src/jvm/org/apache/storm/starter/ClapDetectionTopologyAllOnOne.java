package org.apache.storm.starter;

import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.generated.StormTopology;
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

        // Add sink to possibly route to cloud.
        TupleToMessage sensorSinkScheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) {
                System.out.println("Input from sensor spout was " + input.getString(0));
                return input.getString(0).getBytes();
            }
        };
        ProducerConfig sensorSinkConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, "sensor-sink", debug, sink_is_cloud);
        builder.setBolt("sensor-sink", new RabbitMQBolt(sensorSinkScheme))
                .addConfigurations(sensorSinkConfig.asMap())
                .shuffleGrouping("sensor-spout");
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

        builder.setBolt("splitter2", new ClapDetectionTopology.SplitterBolt(), 1).shuffleGrouping("clap2");

        // RabbitMQ as Sink, message attributes are non-dynamic
        TupleToMessage sink2Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) { return input.getStringByField("detection").getBytes(); }
        };

        // Rabbit Sink as Application Output
        suffix = "sink2";

        ProducerConfig sink2CloudConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, suffix, debug, true);
        ProducerConfig sink2EdgeConfig = ClapDetectionTopology.CreateRabbitSinkConfig(cloud, token, suffix, debug, false);

        builder.setBolt("sink2-cloud", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2CloudConfig.asMap())
                .shuffleGrouping("splitter2", "cloud-stream");
        builder.setBolt("sink2-edge", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2EdgeConfig.asMap())
                .shuffleGrouping("splitter2", "edge-stream");

        return builder.createTopology();
    }
}

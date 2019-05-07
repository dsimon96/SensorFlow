package org.apache.storm.starter;

import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

public class ClapDetectionTopologyHalfAndHalf {

    // debug = false does not use a vhost.
    public static StormTopology CreateClapDetectionTopologyHalfAndHalf(boolean cloud, String token, boolean debug) {
        TopologyBuilder builder = new TopologyBuilder();
        String suffix = "info";
        boolean SINK_IS_CLOUD = cloud;

        /* Begin RabbitMQ as Sensor Input */
        if (!debug) suffix = "sensor-spout";
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
        ProducerConfig sensorSinkConfig = ClapDetectionTopology.CreateRabbitSinkConfig(false, token, "sensor-sink", debug, SINK_IS_CLOUD);
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

        // Midbolt Sink and Spout
        TupleToMessage sink1Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) {
                String volAvgStr = Float.toString(input.getFloatByField("vol_avg"));
                String currVolStr = Float.toString(input.getFloatByField("curr_vol"));
                return (volAvgStr + ";" + currVolStr).getBytes();
            }
        };
        ProducerConfig sink1Config = ClapDetectionTopology.CreateRabbitSinkConfig(true, token, "sink1", debug, SINK_IS_CLOUD);

        builder.setBolt("sink1", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1Config.asMap())
                .shuffleGrouping("clap1");

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
        if (!debug) suffix = "sink2";
        else suffix = "info";
        ProducerConfig sink2Config = ClapDetectionTopology.CreateRabbitSinkConfig(true, token, suffix, debug, SINK_IS_CLOUD);

        builder.setBolt("sink2", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2Config.asMap())
                .shuffleGrouping("clap2");

        return builder.createTopology();
    }
}

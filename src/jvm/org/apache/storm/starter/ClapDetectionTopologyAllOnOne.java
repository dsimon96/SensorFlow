package org.apache.storm.starter;

import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.Scheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

class ClapDetectionTopologyAllOnOne {

    private static String rabbitmqExchange = "sf.topic";
    private static String rabbitmqHost = "localhost";
    private static int rabbitmqPort = 5672;
    private static String rabbitmqUsername = "sf-admin";
    private static String rabbitmqPassword = "buzzword";
    private static String rabbitmqEdgeVhost = "edge";
    private static String rabbitmqCloudVhost = "cloud";

    private static IRichSpout createRabbitSpout(boolean cloud, String token, String suffix) {
        String routingKey = createRoutingKey(cloud, token, suffix);

        // RabbitMQ as Spout
        Scheme scheme = new SensorFlowMessageScheme();
        Declarator declarator = new SensorFlowStormDeclarator(rabbitmqExchange, "", routingKey);
        return new RabbitMQSpout(scheme, declarator);
    }

    private static ConsumerConfig createRabbitSpoutConfig(boolean cloud, boolean debug) {
        String Vhost;
        if (cloud) Vhost = rabbitmqCloudVhost;
        else Vhost = rabbitmqEdgeVhost;

        ConnectionConfig connectionConfig;
        if (debug) connectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, Vhost, 10);
        else connectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqUsername, rabbitmqPassword);
        return new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("") // No queue name
                .prefetch(200)
                .requeueOnFail()
                .build();
    }

    private static ProducerConfig createRabbitSinkConfig(boolean cloud, String token, String suffix, boolean debug, boolean sink_is_cloud) {
        String Vhost;  String routingKey;
        if (cloud) Vhost = rabbitmqCloudVhost;
        else Vhost = rabbitmqEdgeVhost;
        if (sink_is_cloud) routingKey = createRoutingKey(true, token, suffix);
        else routingKey = createRoutingKey(false, token, suffix);

        ConnectionConfig sinkConnectionConfig;
        if (debug) sinkConnectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, Vhost, 10);
        else sinkConnectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqUsername, rabbitmqPassword);
        return new ProducerConfigBuilder()
                .connection(sinkConnectionConfig)
                .contentEncoding("UTF-8")
                .contentType("application/json")
                .exchange(rabbitmqExchange)
                .routingKey(routingKey)
                .build();
    }

    private static String createRoutingKey(boolean cloud, String token, String suffix) {
        if (cloud) return "cloud." + token + "." + suffix;
        return "edge." + token + "." + suffix;
    }

    // debug = false does not use a vhost.
    static StormTopology CreateClapDetectionTopologyAllOnOne(boolean cloud, String token, boolean debug, boolean sink_is_cloud) {
        TopologyBuilder builder = new TopologyBuilder();
        /* Begin RabbitMQ as Sensor Input */
        String suffix = "sensor-spout";
        IRichSpout sensorSpout = createRabbitSpout(cloud, token, suffix); // Receiving sensor data, always edge.
        ConsumerConfig sensorSpoutConfig = createRabbitSpoutConfig(cloud, debug); // Receiving sensor data, always edge.
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
        ProducerConfig sensorSinkConfig = createRabbitSinkConfig(cloud, token, "sensor-sink", debug, sink_is_cloud);
        builder.setBolt("sensor-sink", new RabbitMQBolt(sensorSinkScheme))
                .addConfigurations(sensorSinkConfig.asMap())
                .shuffleGrouping("sensor-spout");
        /* End RabbitMQ as Sensor Input */

        IRichSpout spout1 = createRabbitSpout(cloud, token, "sensor-sink");
        ConsumerConfig spout1Config = createRabbitSpoutConfig(cloud, debug);
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
        ProducerConfig sink1Config = createRabbitSinkConfig(cloud, token, "sink1", debug, sink_is_cloud);

        builder.setBolt("sink1", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1Config.asMap())
                .shuffleGrouping("clap1");

        IRichSpout spout2 = createRabbitSpout(cloud, token, "sink1");
        ConsumerConfig spout2Config = createRabbitSpoutConfig(cloud, debug);
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

        ProducerConfig sink2Config = createRabbitSinkConfig(cloud, token, suffix, debug, sink_is_cloud);

        builder.setBolt("sink2", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2Config.asMap())
                .shuffleGrouping("clap2");

        return builder.createTopology();
    }
}

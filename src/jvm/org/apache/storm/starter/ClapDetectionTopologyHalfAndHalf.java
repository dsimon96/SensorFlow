package org.apache.storm.starter;

import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.Scheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

public class ClapDetectionTopologyHalfAndHalf {

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

    private static ProducerConfig createRabbitSinkConfig(boolean cloud, String token, String suffix, boolean debug) {
        String Vhost;
        if (cloud) Vhost = rabbitmqCloudVhost;
        else Vhost = rabbitmqEdgeVhost;
        String routingKey = createRoutingKey(cloud, token, suffix);

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
    public static StormTopology CreateClapDetectionTopologyHalfAndHalf(String token, boolean debug) {
        TopologyBuilder builder = new TopologyBuilder();
        String suffix = "info";

        /* Begin RabbitMQ as Sensor Input */
        if (!debug) suffix = "sensor-spout";
        IRichSpout sensorSpout = createRabbitSpout(false, token, suffix); // Receiving sensor data, always edge.
        ConsumerConfig sensorSpoutConfig = createRabbitSpoutConfig(false, debug); // Receiving sensor data, always edge.
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
        ProducerConfig sensorSinkConfig = createRabbitSinkConfig(false, token, "sensor-sink", debug);
        builder.setBolt("sensor-sink", new RabbitMQBolt(sensorSinkScheme))
                .addConfigurations(sensorSinkConfig.asMap())
                .shuffleGrouping("sensor-spout");
        /* End RabbitMQ as Sensor Input */

        IRichSpout spout1 = createRabbitSpout(false, token, "sensor-sink");
        ConsumerConfig spout1Config = createRabbitSpoutConfig(false, debug);
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
        ProducerConfig sink1Config = createRabbitSinkConfig(true, token, "sink1", debug);

        builder.setBolt("sink1", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1Config.asMap())
                .shuffleGrouping("clap1");

        IRichSpout spout2 = createRabbitSpout(true, token, "sink1");
        ConsumerConfig spout2Config = createRabbitSpoutConfig(true, debug);
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
        ProducerConfig sink2Config = createRabbitSinkConfig(true, token, suffix, debug);

        builder.setBolt("sink2", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2Config.asMap())
                .shuffleGrouping("clap2");

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        String token = "fake-token";

        StormTopology topology = CreateClapDetectionTopologyHalfAndHalf(token, true);

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("clapDetectionTopology", conf, topology);

            // Remove below lines to run indefinitely.
            //Utils.sleep(100000);
            //cluster.killTopology("clapDetectionTopology");
            //cluster.shutdown();
        }
    }
}

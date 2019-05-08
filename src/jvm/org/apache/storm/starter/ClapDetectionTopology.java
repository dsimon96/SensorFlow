package org.apache.storm.starter;

import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.Scheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class ClapDetectionTopology {

    private static String rabbitmqExchange = "sf.topic";
    private static String rabbitmqHost = "localhost";
    private static int rabbitmqPort = 5672;
    private static String rabbitmqUsername = "sf-admin";
    private static String rabbitmqPassword = "buzzword";
    private static String rabbitmqEdgeVhost = "edge";
    private static String rabbitmqCloudVhost = "cloud";

    // Calculates the average volume in the recent past, and passes on the current volume.
    public static class ClapDetection1Bolt extends BaseRichBolt {
        OutputCollector _collector;
        String boltId;

        int maxQueueSize = 10;
        float volumeTotal = (float) 0.0;
        Queue<Float> pastVolumesQueue = new LinkedList<>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            boltId = context.getThisComponentId();
        }

        @Override
        public void execute(Tuple tuple) {
            Instant startTs = Instant.now();
            Utils.sleep(100);
            try {
                float volume = Float.parseFloat(tuple.getString(0));
                pastVolumesQueue.add(new Float(volume));
                volumeTotal += volume;
                if (pastVolumesQueue.size() > maxQueueSize) { // Only count last maxQueueSize volumes in total.
                    Float oldestVolume = pastVolumesQueue.remove();
                    volumeTotal -= oldestVolume.floatValue();
                }

                float volumeAverage = volumeTotal / pastVolumesQueue.size();
                System.out.println("from clap1, emitting " + volumeAverage + " and " + volume);

                String output = Float.toString(volumeAverage) + ";" + Float.toString(volume);
                Instant endTs = Instant.now();
                Duration duration = Duration.between(startTs, endTs);
                WriteTimestampToFile(boltId, duration.toMillis());
                _collector.emit(new Values(output));
                _collector.ack(tuple);
            }
            catch (Exception e) {
                System.out.println("No input for ClapDetection1Bolt");
                System.out.println(e.toString());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("detection")); // Declares the output fields for the component
        }
    }

    public static class ClapDetection2Bolt extends BaseRichBolt {
        OutputCollector _collector;
        String boltId;

        float thresholdDifference = (float) 3.0;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            boltId = context.getThisComponentId();
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Instant startTs = Instant.now();
            Utils.sleep(100);
            try {
                String midboltMsg = tuple.getString(0);
                String[] splitMsg = midboltMsg.split(";", 2);
                float volumeAverage = Float.parseFloat(splitMsg[0]);
                float currentVolume = Float.parseFloat(splitMsg[1]);

                System.out.println("at clap2, received " + volumeAverage + " and " + currentVolume);

                float currentDifference = currentVolume - volumeAverage;

                Instant endTs;
                if (currentDifference > thresholdDifference) { // Determine if there was a clap.
                    endTs = Instant.now();
                    _collector.emit(new Values("clap detected!"));
                } else {
                    endTs = Instant.now();
                    _collector.emit(new Values("nothing"));
                }
                Duration duration = Duration.between(startTs, endTs);
                WriteTimestampToFile(boltId, duration.toMillis());
                _collector.ack(tuple);
            }
            catch (Exception e) {
                System.out.println("No input for ClapDetection2Bolt");
                System.out.println(e.toString());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("detection")); // Declares the output fields for the component
        }
    }

    public static class SplitterBolt extends BaseRichBolt {
        OutputCollector _collector;

        int item_count = 0;
        int item_check_interval = 10;
        boolean choose_random = false;
        boolean destination_cloud = false;
        String boltId;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            boltId = context.getThisComponentId();
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                if (item_count % item_check_interval == 0) {
                    File file = new File("src/" + boltId + ".txt");

                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line;
                    while ((line = br.readLine()) != null) {
                        System.out.println("SPLITTER: line from file="+line);
                        if (line.contains("edge")) destination_cloud = false;
                        else if (line.contains("cloud")) destination_cloud = true;
                        if (line.contains("random")) choose_random = true;
                        else choose_random = false;
                    }
                }
                item_count++;

                if (choose_random) {
                    if (Math.random() < .5) {
                        destination_cloud = false;
                    } else {
                        destination_cloud = true;
                    }
                }

                System.out.print("at " + boltId + ", sending to ");
                if (destination_cloud) {
                    System.out.println("cloud");
                    _collector.emit("cloud-stream", tuple, new Values(tuple.getValue(0)));
                } else {
                    System.out.println("edge");
                    _collector.emit("edge-stream", tuple, new Values(tuple.getValue(0)));
                }
                _collector.ack(tuple);
            }
            catch (Exception e) {
                System.out.println("No input for SplitterBolt");
                System.out.println(e.toString());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("cloud-stream", new Fields("detection"));
            declarer.declareStream("edge-stream", new Fields("detection"));
            declarer.declare(new Fields("detection")); // Declares the output fields for the component
        }
    }

    public static IRichSpout CreateRabbitSpout(boolean cloud, String token, String suffix) {
        String routingKey = CreateRoutingKey(cloud, token, suffix);

        // RabbitMQ as Spout
        Scheme scheme = new SensorFlowMessageScheme();
        Declarator declarator = new SensorFlowStormDeclarator(rabbitmqExchange, "", routingKey);
        return new RabbitMQSpout(scheme, declarator);
    }

    public static ConsumerConfig CreateRabbitSpoutConfig(boolean cloud, boolean debug) {
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

    public static ProducerConfig CreateRabbitSinkConfig(boolean cloud, String token, String suffix, boolean debug, boolean sink_is_cloud) {
        String Vhost;  String routingKey;
        if (cloud) Vhost = rabbitmqCloudVhost;
        else Vhost = rabbitmqEdgeVhost;
        if (sink_is_cloud) routingKey = CreateRoutingKey(true, token, suffix);
        else routingKey = CreateRoutingKey(false, token, suffix);

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

    public static String CreateRoutingKey(boolean cloud, String token, String suffix) {
        if (cloud) return "cloud." + token + "." + suffix;
        return "edge." + token + "." + suffix;
    }

    // Gets ts in nanoseconds, writes as milliseconds.
    public static void WriteTimestampToFile(String dataBoltId, long ts) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("src/latency-" + dataBoltId + ".txt", true));
            writer.write(Long.toString(ts));
            writer.newLine();
            writer.close();
        } catch (Exception e) {
            System.out.println("Error when writing to file: " + e.toString());
        }
    }

    // debug = false does not use a vhost.
    public static StormTopology CreateClapDetectionTopology(boolean cloud, String token, boolean debug) {
        TopologyBuilder builder = new TopologyBuilder();
        String suffix = "info";
        boolean SINK_IS_CLOUD = cloud;

        /* Begin RabbitMQ as Sensor Input */
        if (!debug) suffix = "sensor-spout";
        IRichSpout sensorSpout = CreateRabbitSpout(false, token, suffix); // Receiving sensor data, always edge.
        ConsumerConfig sensorSpoutConfig = CreateRabbitSpoutConfig(false, debug); // Receiving sensor data, always edge.
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
        ProducerConfig sensorSinkConfig = CreateRabbitSinkConfig(cloud, token, "sensor-sink", debug, SINK_IS_CLOUD);
        builder.setBolt("sensor-sink", new RabbitMQBolt(sensorSinkScheme))
                .addConfigurations(sensorSinkConfig.asMap())
                .shuffleGrouping("sensor-spout");
        /* End RabbitMQ as Sensor Input */

        IRichSpout spout1 = CreateRabbitSpout(cloud, token, "sensor-sink");
        ConsumerConfig spout1Config = CreateRabbitSpoutConfig(cloud, debug);
        builder.setSpout("spout1", spout1, 1)
                .addConfigurations(spout1Config.asMap())
                .setMaxSpoutPending(200);


        builder.setBolt("clap1", new ClapDetection1Bolt(), 1).shuffleGrouping("spout1");

        // Midbolt Sink and Spout
        TupleToMessage sink1Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) {
                String volAvgStr = Float.toString(input.getFloatByField("vol_avg"));
                String currVolStr = Float.toString(input.getFloatByField("curr_vol"));
                return (volAvgStr + ";" + currVolStr).getBytes();
            }
        };
        ProducerConfig sink1Config = CreateRabbitSinkConfig(cloud, token, "sink1", debug, SINK_IS_CLOUD);

        builder.setBolt("sink1", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1Config.asMap())
                .shuffleGrouping("clap1");

        IRichSpout spout2 = CreateRabbitSpout(cloud, token, "sink1");
        ConsumerConfig spout2Config = CreateRabbitSpoutConfig(cloud, debug);
        builder.setSpout("spout2", spout2, 1)
                .addConfigurations(spout2Config.asMap())
                .setMaxSpoutPending(200);

        builder.setBolt("clap2", new ClapDetection2Bolt(), 1).shuffleGrouping("spout2");

        // RabbitMQ as Sink, message attributes are non-dynamic
        TupleToMessage sink2Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) { return input.getStringByField("detection").getBytes(); }
        };

        // Rabbit Sink as Application Output
        if (!debug) suffix = "sink2";
        if (debug) {
            suffix = "info";
            cloud = true; // Output to cloud for testing.
        }
        ProducerConfig sink2Config = CreateRabbitSinkConfig(cloud, token, suffix, debug, SINK_IS_CLOUD);

        builder.setBolt("sink2", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2Config.asMap())
                .shuffleGrouping("clap2");

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        boolean edge = false;
        String token = "fake-token";
        boolean debug = true;

        StormTopology topology = CreateClapDetectionTopology(edge, token, debug);

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

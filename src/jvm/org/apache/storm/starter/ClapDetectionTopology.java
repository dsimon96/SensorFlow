package org.apache.storm.starter;

import io.latent.storm.rabbitmq.*;
import io.latent.storm.rabbitmq.config.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
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

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class ClapDetectionTopology {

    private static String rabbitmqExchange = "sf.topic";
    private static String rabbitmqSpoutRoutingKey = "edge.info";
    private static String rabbitmqMidboltRoutingKey = "midbolt.info";
    private static String rabbitmqSinkRoutingKey = "cloud.info";
    private static String rabbitmqHost = "localhost";
    private static int rabbitmqPort = 5672;
    private static String rabbitmqUsername = "sf-admin";
    private static String rabbitmqPassword = "buzzword";
    private static String rabbitmqEdgeVhost = "edge";
    private static String rabbitmqCloudVhost = "cloud";

    // Calculates the average volume in the recent past, and passes on the current volume.
    public static class ClapDetection1Bolt extends BaseRichBolt {
        OutputCollector _collector;

        int maxQueueSize = 10;
        float volumeTotal = (float) 0.0;
        Queue<Float> pastVolumesQueue = new LinkedList<>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
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
                _collector.emit(new Values(volumeAverage, volume));
                _collector.ack(tuple);
            }
            catch (Exception e) {
                System.out.println("No input for ClapDetection1Bolt");
                System.out.println(e.toString());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("vol_avg", "curr_vol")); // Declares the output fields for the component
        }
    }

    public static class ClapDetection2Bolt extends BaseRichBolt {
        OutputCollector _collector;

        float thresholdDifference = (float) 3.0;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                //float volumeAverage = tuple.getFloat(0).floatValue();
                //float currentVolume = tuple.getFloat(1).floatValue();
                String midboltMsg = tuple.getString(0);
                String[] splitMsg = midboltMsg.split(";", 2);
                float volumeAverage = Float.parseFloat(splitMsg[0]);
                float currentVolume = Float.parseFloat(splitMsg[1]);

                System.out.println("at clap2, received " + volumeAverage + " and " + currentVolume);

                float currentDifference = currentVolume - volumeAverage;

                if (currentDifference > thresholdDifference) { // Determine if there was a clap.
                    _collector.emit(new Values("clap detected!"));
                } else {
                    _collector.emit(new Values("nothing"));
                }
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

    public static IRichSpout CreateRabbitSpout(String spoutRoutingKey) {
        // RabbitMQ as Spout
        Scheme scheme = new SensorFlowMessageScheme();
        Declarator declarator = new SensorFlowStormDeclarator(rabbitmqExchange, "", spoutRoutingKey);
        return new RabbitMQSpout(scheme, declarator);
    }

    public static ConsumerConfig CreateRabbitSpoutConfig(String Vhost) {
        ConnectionConfig connectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, Vhost, 10);
        return new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("") // No queue name
                .prefetch(200)
                .requeueOnFail()
                .build();
    }

    public static ProducerConfig CreateRabbitSinkConfig(String Vhost, String sinkRoutingKey) {
        ConnectionConfig sinkConnectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, Vhost, 10);
        return new ProducerConfigBuilder()
                .connection(sinkConnectionConfig)
                .contentEncoding("UTF-8")
                .contentType("application/json")
                .exchange(rabbitmqExchange)
                .routingKey(sinkRoutingKey)
                .build();
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Rabbit Spout as Sensor Input
        IRichSpout spout1 = CreateRabbitSpout(rabbitmqSpoutRoutingKey);
        ConsumerConfig spout1Config = CreateRabbitSpoutConfig(rabbitmqEdgeVhost);
        builder.setSpout("rabbit-spout1", spout1, 1)
            .addConfigurations(spout1Config.asMap())
            .setMaxSpoutPending(200);

        builder.setBolt("clap1", new ClapDetection1Bolt(), 1).shuffleGrouping("rabbit-spout1");

        // Midbolt Sink and Spout
        TupleToMessage sink1Scheme = new TupleToMessageNonDynamic() {
            @Override
            public byte[] extractBody(Tuple input) {
                String volAvgStr = Float.toString(input.getFloatByField("vol_avg"));
                String currVolStr = Float.toString(input.getFloatByField("curr_vol"));
                return (volAvgStr + ";" + currVolStr).getBytes();
            }
        };
        ProducerConfig sink1Config = CreateRabbitSinkConfig(rabbitmqEdgeVhost, rabbitmqMidboltRoutingKey);

        builder.setBolt("rabbit-sink1", new RabbitMQBolt(sink1Scheme))
                .addConfigurations(sink1Config.asMap())
                .shuffleGrouping("clap1");

        IRichSpout spout2 = CreateRabbitSpout(rabbitmqMidboltRoutingKey);
        ConsumerConfig spout2Config = CreateRabbitSpoutConfig(rabbitmqEdgeVhost);
        builder.setSpout("rabbit-spout2", spout2, 1)
                .addConfigurations(spout2Config.asMap())
                .setMaxSpoutPending(200);


        builder.setBolt("clap2", new ClapDetection2Bolt(), 1).shuffleGrouping("rabbit-spout2");

        // RabbitMQ as Sink, message attributes are non-dynamic
        TupleToMessage sink2Scheme = new TupleToMessageNonDynamic() {
          @Override
          public byte[] extractBody(Tuple input) { return input.getStringByField("detection").getBytes(); }
        };

        // Rabbit Sink as Application Output
        ProducerConfig sink2Config = CreateRabbitSinkConfig(rabbitmqCloudVhost, rabbitmqSinkRoutingKey);

        builder.setBolt("rabbit-sink2", new RabbitMQBolt(sink2Scheme))
                .addConfigurations(sink2Config.asMap())
                .shuffleGrouping("clap2");


        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("clapDetectionTopology", conf, builder.createTopology());

            // Remove below lines to run indefinitely.
            //Utils.sleep(100000);
            //cluster.killTopology("clapDetectionTopology");
            //cluster.shutdown();
        }
    }
}

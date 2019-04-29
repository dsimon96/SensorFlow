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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class ClapDetectionTopology {

    private static String rabbitmqExchange = "sf.topic";
    private static String rabbitmqSpoutRoutingKey = "edge.info";
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
                float volumeAverage = tuple.getFloat(0).floatValue();
                float currentVolume = tuple.getFloat(1).floatValue();

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

    // From int to byte array converter from
    // https://stackoverflow.com/questions/1936857/convert-integer-into-byte-array-java
    private static byte[] intToBytes( final int i ) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // RabbitMQ as Spout
        Scheme scheme = new SensorFlowMessageScheme();
        Declarator declarator = new SensorFlowStormDeclarator(rabbitmqExchange, "", rabbitmqSpoutRoutingKey);
        IRichSpout spout = new RabbitMQSpout(scheme, declarator);

        ConnectionConfig connectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, rabbitmqEdgeVhost, 10);
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("") // No queue name
                .prefetch(200)
                .requeueOnFail()
                .build();

        builder.setSpout("rabbit1", spout, 1)
            .addConfigurations(spoutConfig.asMap())
            .setMaxSpoutPending(200);

        builder.setBolt("clap1", new ClapDetection1Bolt(), 1).shuffleGrouping("rabbit1");
        builder.setBolt("clap2", new ClapDetection2Bolt(), 1).shuffleGrouping("clap1");

        Config conf = new Config();
        conf.setDebug(true);

        /* RabbitMQ as Sink, dynamic */
        /*TupleToMessage sinkScheme = new TupleToMessage() {
            @Override
            public byte[] extractBody(Tuple input) { return input.getStringByField("word").getBytes(); }

            @Override
            public String determineExchangeName(Tuple input) { return input.getStringByField(rabbitmqExchange); }

            @Override
            public String determineRoutingKey(Tuple input) { return input.getStringByField(rabbitmqSinkRoutingKey); }

            //@Override
            //public Map<String, Object> specifiyHeaders(Tuple input) { return new HashMap<String, Object>(); }

            @Override
            public String specifyContentType(Tuple input) { return "application/json"; }

            @Override
            public String specifyContentEncoding(Tuple input) { return "UTF-8"; }

            @Override
            public boolean specifyMessagePersistence(Tuple input) { return false; }
        };
        ConnectionConfig sinkConnectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, rabbitmqCloudVhost, 10);
        ProducerConfig sinkConfig = new ProducerConfigBuilder().connection(sinkConnectionConfig).build();
        builder.setBolt("rabbitmq-sink", new RabbitMQBolt(sinkScheme))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping("frame1");
        */

        /* RabbitMQ as Sink, message attributes are non-dynamic */
        TupleToMessage sinkScheme = new TupleToMessageNonDynamic() {
          @Override
          public byte[] extractBody(Tuple input) { return input.getStringByField("detection").getBytes(); }
        };

        ConnectionConfig sinkConnectionConfig = new ConnectionConfig(rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword, rabbitmqCloudVhost, 10);
        ProducerConfig sinkConfig = new ProducerConfigBuilder()
                .connection(sinkConnectionConfig)
                .contentEncoding("UTF-8")
                .contentType("application/json")
                .exchange(rabbitmqExchange)
                .routingKey(rabbitmqSinkRoutingKey)
                .build();

        builder.setBolt("rabbitmq-sink", new RabbitMQBolt(sinkScheme))
                .addConfigurations(sinkConfig.asMap())
                .shuffleGrouping("clap2");

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

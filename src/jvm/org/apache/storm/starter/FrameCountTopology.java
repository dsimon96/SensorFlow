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
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FrameCountTopology {

    private static String rabbitmqExchange = "sf.topic";
    private static String rabbitmqSpoutRoutingKey = "edge.info";
    private static String rabbitmqSinkRoutingKey = "cloud.info";
    private static String rabbitmqHost = "localhost";
    private static int rabbitmqPort = 5672;
    private static String rabbitmqUsername = "sf-admin";
    private static String rabbitmqPassword = "buzzword";
    private static String rabbitmqEdgeVhost = "edge";
    private static String rabbitmqCloudVhost = "cloud";

    public static class FrameCountBolt extends BaseRichBolt {
        OutputCollector _collector;
        Map<String, Integer> counts = new HashMap<>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                String word = tuple.getString(0);
                Integer count = counts.get(word);
                if (count == null)
                    count = 0;
                count++;
                counts.put(word, count);
                _collector.emit(new Values(word, count));
                _collector.ack(tuple);
            }
            catch (Exception e) {
                System.out.println("No input for FrameCountBolt");
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count")); // Declares the output fields for the component
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
        Scheme scheme = new OurCustomMessageScheme();
        Declarator declarator = new OurCustomStormDeclarator(rabbitmqExchange, "", rabbitmqSpoutRoutingKey);
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

        // Put frame logic code into FrameCountBolt.
        builder.setBolt("frame1", new FrameCountBolt(), 1).shuffleGrouping("rabbit1");
        //builder.setBolt("frame2", new FrameCountBolt(), 1).shuffleGrouping("frame1");

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
          public byte[] extractBody(Tuple input) { return intToBytes(input.getIntegerByField("count")); }
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
                .shuffleGrouping("frame1");
         //*/



        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("frameTopology", conf, builder.createTopology());

            // Remove below lines to run indefinitely.
            Utils.sleep(100000);
            cluster.killTopology("frameTopology");
            cluster.shutdown();
        }
    }
}

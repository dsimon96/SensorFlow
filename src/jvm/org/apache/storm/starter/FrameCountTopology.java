package org.apache.storm.starter;

import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
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

import java.util.HashMap;
import java.util.Map;

public class FrameCountTopology {

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
            declarer.declare(new Fields("word")); // Declares the output fields for the component
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        Scheme scheme = new OurCustomMessageScheme();
        IRichSpout spout = new RabbitMQSpout(scheme);

        ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 15712, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("your.rabbitmq.queue")
                .prefetch(200)
                .requeueOnFail()
                .build();

        builder.setSpout("rabbit1", spout, 1)
            .addConfigurations(spoutConfig.asMap())
            .setMaxSpoutPending(200);
        builder.setBolt("frame1", new FrameCountBolt(), 1).shuffleGrouping("rabbit1");
        builder.setBolt("frame2", new FrameCountBolt(), 1).shuffleGrouping("frame1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("frameTopology", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("frameTopology");
            cluster.shutdown();
        }
    }
}

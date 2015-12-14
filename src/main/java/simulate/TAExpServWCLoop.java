package simulate;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import util.ConfigUtil;

import static topology.StormConfigManager.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class TAExpServWCLoop {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 10);

        String host = getString(conf, "redis.host");
        int port = getInt(conf, "redis.port");
        String queueName = getString(conf, "redis.sourceQueueName");

        builder.setSpout("loop-Spout", new TASentenceSpout(host, port, queueName),
                ConfigUtil.getInt(conf, "loop-Spout.parallelism", 1));

        double loopBoltA_mu = ConfigUtil.getDouble(conf, "loop-BoltA.mu", 1.0);
        builder.setBolt("loop-BoltA", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / loopBoltA_mu)),
                ConfigUtil.getInt(conf, "loop-BoltA.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("loop-Spout");

        double loopBoltB_mu = ConfigUtil.getDouble(conf, "loop-BoltB.mu", 1.0);
        builder.setBolt("loop-BoltB",
                new TAWordCounter2Path(() -> (long) (-Math.log(Math.random()) * 1000.0 / loopBoltB_mu),
                        ConfigUtil.getDouble(conf, "loop-BoltB-loopback.prob", 0.0)),
                ConfigUtil.getInt(conf, "loop-BoltB.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("loop-BoltA")
                .shuffleGrouping("loop-BoltB", "P-Stream");
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));

        if (ConfigUtil.getBoolean(conf, "EnableLoggingMetricsConsumer", false)) {
            System.out.printf("LoggingMetricsConsumer is enabled");
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        StormSubmitter.submitTopology("loop-top-1", conf, builder.createTopology());
    }
}

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
public class TAExpServWC {


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

        builder.setSpout("chain-Spout", new TASentenceSpout(host, port, queueName),
                ConfigUtil.getInt(conf, "chain-spout.parallelism", 1));

        double chainBoltA_mu = ConfigUtil.getDouble(conf, "chain-BoltA.mu", 1.0);
        builder.setBolt("chain-BoltA", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / chainBoltA_mu)),
                ConfigUtil.getInt(conf, "chain-BoltA.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("chain-Spout");

        double chainBoltB_mu = ConfigUtil.getDouble(conf, "chain-BoltB.mu", 1.0);
        builder.setBolt("chain-BoltB", new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / chainBoltB_mu)),
                ConfigUtil.getInt(conf, "chain-BoltB.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("chain-BoltA");

        conf.setNumWorkers(getInt(conf, "chain-NumOfWorkers"));
        conf.setMaxSpoutPending(getInt(conf, "chain-MaxSpoutPending"));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));

        if (ConfigUtil.getBoolean(conf, "EnableLoggingMetricsConsumer", false)) {
            System.out.printf("LoggingMetricsConsumer is enabled");
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        StormSubmitter.submitTopology("chain-top-1", conf, builder.createTopology());
    }
}

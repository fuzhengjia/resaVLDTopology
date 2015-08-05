package showTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import tool.FrameSource;
import tool.Serializable;

import java.io.FileNotFoundException;

import static tool.Constants.STREAM_FRAME_OUTPUT;
import static topology.StormConfigManager.*;

/**
 * Created by Intern04 on 4/8/2014.
 */
public class tomSimpleTestTopology {

    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);
        int numberOfWorkers = getInt(conf, "st-numberOfWorkers");
        //int numberOfAckers = getInt(conf, "numberOfAckers");

        TopologyBuilder builder = new TopologyBuilder();

        String host = getString(conf, "redis.host");
        int port = getInt(conf, "redis.port");
        String queueName = getString(conf, "redis.sourceQueueName");

        builder.setSpout("fSource", new FrameSource(host, port, queueName), getInt(conf, "ShowTrajSpout.parallelism"))
                .setNumTasks(getInt(conf, "ShowTrajSpout.tasks"));

        builder.setBolt("simpleTrans", new SimpleTransBolt(), getInt(conf, "SimpleTransBolt.parallelism"))
                .shuffleGrouping("fSource", STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, "SimpleTransBolt.tasks"));

        builder.setBolt("fOut", new RedisFrameOutput(), getInt(conf, "ShowTrajAggregatorBolt.parallelism"))
                .globalGrouping("simpleTrans", STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, "ShowTrajAggregatorBolt.tasks"));

        StormTopology topology = builder.createTopology();

        conf.setNumWorkers(numberOfWorkers);
        //conf.setNumAckers(numberOfAckers);
        conf.setMaxSpoutPending(getInt(conf, "st-MaxSpoutPending"));
        conf.registerSerialization(Serializable.Mat.class);

        StormSubmitter.submitTopology("tomSimpleTestTopology", conf, topology);

    }
}

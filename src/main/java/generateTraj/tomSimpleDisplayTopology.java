package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import showTraj.RedisFrameOutput;
import tool.FrameImplImageSource;
import topology.Serializable;

import java.io.FileNotFoundException;

import static tool.Constant.STREAM_FRAME_OUTPUT;
import static tool.Constant.STREAM_OPT_FLOW;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Jan 29, 2015.
 */
public class tomSimpleDisplayTopology {

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

        builder.setSpout("fSource", new FrameImplImageSource(host, port, queueName), getInt(conf, "GenTrajSpout.parallelism"))
                .setNumTasks(getInt(conf, "GenTrajSpout.tasks"));

        builder.setBolt("fOptFlow", new opticalFlowCalculator(), getInt(conf, "GenTrajOptFlow.parallelism"))
                .globalGrouping("fSource", STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, "GenTrajOptFlow.tasks"));

        builder.setBolt("fOptFlow", new RedisFrameOutput(), getInt(conf, "GenTrajFrameOutput.parallelism"))
                .globalGrouping("fSource", STREAM_OPT_FLOW)
                .setNumTasks(getInt(conf, "GenTrajFrameOutput.tasks"));

        StormTopology topology = builder.createTopology();

        conf.setNumWorkers(numberOfWorkers);
        //conf.setNumAckers(numberOfAckers);
        conf.setMaxSpoutPending(getInt(conf, "st-MaxSpoutPending"));

        conf.registerSerialization(Serializable.Mat.class);
        StormSubmitter.submitTopology("tomSimpleDisplayTopology", conf, topology);

    }
}

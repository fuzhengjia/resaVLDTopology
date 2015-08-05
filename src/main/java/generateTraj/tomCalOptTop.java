package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import showTraj.RedisFrameOutputByteArr;
import tool.FrameMatSourceOptFlow;
import tool.Serializable;

import java.io.FileNotFoundException;

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Mar 3, 2015.
 * TODO: Notes:
 * 此版本暂时通过测试
 * 这个topology只是单独产生opt flow， 然后将optflow和rawframe以byte【】的形式output
 * 目前以1：6：1 （3 workers）的配备达到25fps
 */
public class tomCalOptTop {

    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);

        TopologyBuilder builder = new TopologyBuilder();

        String host = getString(conf, "redis.host");
        int port = getInt(conf, "redis.port");
        String queueName = getString(conf, "redis.sourceQueueName");

        String spoutName = "of-Spout";
        String optFlowGenBolt = "of-Generator";
        String redisFrameOut = "of-RedisFrameOut";

        builder.setSpout(spoutName, new FrameMatSourceOptFlow(host, port, queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(optFlowGenBolt, new GeneratingOptFlowBolt(), getInt(conf, optFlowGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, optFlowGenBolt + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutputByteArr(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "of-NumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "of-MaxPending"));

        conf.registerSerialization(Serializable.Mat.class);
//        conf.setStatsSampleRate(1.0);
        StormSubmitter.submitTopology("tomCalOptTop-1", conf, topology);
    }
}

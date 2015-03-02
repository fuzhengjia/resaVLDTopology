package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import showTraj.RedisFrameOutput;
import showTraj.RedisFrameOutputByteArr;
import tool.FrameImplImageSourceGamma;
import tool.FrameMatSourceOptFlow;
import tool.FrameSource;
import topology.Serializable;

import java.io.FileNotFoundException;

import static tool.Constant.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Jan 29, 2015.
 * TODO: Notes:
 * traceGenerator 是否可以并行？ 这样需要feedback分开，register也要分开
 * 扩展，如果有2个scale的话，需要对当前程序扩展！
 * 产生光流是bottleneck
 * 此版本暂时通过测试
 * 尝试将optFlowGen and optFlowAgg 分布式化
 * test Gamma version!
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
        conf.setStatsSampleRate(1.0);
        StormSubmitter.submitTopology("tomCalOptTop-1", conf, topology);
    }
}

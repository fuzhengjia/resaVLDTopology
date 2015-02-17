package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import showTraj.RedisFrameOutput;
import tool.FrameImplImageSourceGamma;
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
public class tomTrajDisplayTopDelta {

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

        String spoutName = "TrajSpout";
        String imgPrepareBolt = "TrajImgPrep";
        String optFlowGenBolt = "TrajOptFlowGen";
        String traceGenBolt = "TrajTraceGen";
        String optFlowTrans = "TrajOptFlowTrans";
        String optFlowTracker = "TrajOptFlowTracker";
        String traceAggregator = "TrajTraceAgg";
        String frameDisplay = "TrajDisplay";
        String redisFrameOut = "RedisFrameOut";

        builder.setSpout(spoutName, new FrameImplImageSourceGamma(host, port, queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(imgPrepareBolt, new imagePrepareGamma2(), getInt(conf, imgPrepareBolt + ".parallelism"))
                .shuffleGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, imgPrepareBolt + ".tasks"));

        builder.setBolt(optFlowGenBolt, new optlFlowGeneratorMultiOptFlow(), getInt(conf, optFlowGenBolt + ".parallelism"))
                .shuffleGrouping(imgPrepareBolt, STREAM_GREY_FLOW)
                .setNumTasks(getInt(conf, optFlowGenBolt + ".tasks"));

        builder.setBolt(optFlowTrans, new optlFlowTrans(optFlowTracker), getInt(conf, optFlowTrans + ".parallelism"))
                .allGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .setNumTasks(getInt(conf, optFlowTrans + ".tasks"));

        builder.setBolt(traceGenBolt, new traceGeneratorDelta(traceAggregator), getInt(conf, traceGenBolt + ".parallelism"))
                .allGrouping(imgPrepareBolt, STREAM_EIG_FLOW)
                .allGrouping(traceAggregator, STREAM_INDICATOR_TRACE)
                .setNumTasks(getInt(conf, traceGenBolt + ".tasks"));

        builder.setBolt(optFlowTracker, new optFlowTrackerDelta(traceAggregator), getInt(conf, optFlowTracker + ".parallelism"))
                .shuffleGrouping(traceGenBolt, STREAM_NEW_TRACE)
                .shuffleGrouping(traceAggregator, STREAM_RENEW_TRACE)
                ///.allGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .directGrouping(optFlowTrans, STREAM_OPT_FLOW)
                .allGrouping(frameDisplay, STREAM_CACHE_CLEAN)
                .setNumTasks(getInt(conf, optFlowTracker + ".tasks"));

        builder.setBolt(traceAggregator, new traceAggregatorDelta(traceGenBolt), getInt(conf, traceAggregator + ".parallelism"))
                .directGrouping(traceGenBolt, STREAM_REGISTER_TRACE)
                .directGrouping(optFlowTracker, STREAM_EXIST_TRACE)
                .directGrouping(optFlowTracker, STREAM_REMOVE_TRACE)
                .setNumTasks(getInt(conf, traceAggregator + ".tasks"));

        builder.setBolt(frameDisplay, new frameDisplayMultiDelta(traceAggregator), getInt(conf, frameDisplay + ".parallelism"))
                .fieldsGrouping(spoutName, STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(traceAggregator, STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, frameDisplay + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutput(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(frameDisplay, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "TrajNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "TrajMaxPending"));

        conf.registerSerialization(Serializable.Mat.class);
        conf.setStatsSampleRate(1.0);
        StormSubmitter.submitTopology("tomTrajDisplayTopDelta-1", conf, topology);
    }
}

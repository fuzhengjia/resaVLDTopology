package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import tool.FrameImplImageSourceFox;
import tool.FrameImplImageSourceGolf;
import tool.RedisFrameOutputFox;
import tool.Serializable;
import util.ConfigUtil;

import java.io.FileNotFoundException;

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Jan 29, 2015.
 * TODO: Notes:
 * 扩展，如果有2个scale的话，需要对当前程序扩展！
 * 在echoBatch里有个大的bug，产生trace的方式有问题
 * 1. 应该由preFrame产生newTrace到当前的optFrame来更新，这个版本里面尝试解决这个问题
 * 2. 第二个bug是在flowTracker里面，对新的trace， 会自动扔掉第一个点！！！
 * 2. 重写一些data structure
 *
 * Golf version 是基于Fox version， 实现一个新功能，按照Peiyong的建议，当输入文件的resolution很大的时候，可以先resize变小，产生trajectory之后，
 * 再显示的时候resize回来。
 */
public class tomTrajDisplayTopGolf {

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

        builder.setSpout(spoutName, new FrameImplImageSourceGolf(host, port, queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(imgPrepareBolt, new imagePrepareGolf(traceGenBolt), getInt(conf, imgPrepareBolt + ".parallelism"))
                .shuffleGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, imgPrepareBolt + ".tasks"));

        builder.setBolt(optFlowGenBolt, new optlFlowGeneratorMultiOptFlow(), getInt(conf, optFlowGenBolt + ".parallelism"))
                .shuffleGrouping(imgPrepareBolt, STREAM_GREY_FLOW)
                .setNumTasks(getInt(conf, optFlowGenBolt + ".tasks"));

        builder.setBolt(optFlowTrans, new optlFlowTransFox(optFlowTracker), getInt(conf, optFlowTrans + ".parallelism"))
                .shuffleGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .setNumTasks(getInt(conf, optFlowTrans + ".tasks"));

        builder.setBolt(traceGenBolt, new traceGenFox(traceAggregator, optFlowTracker), getInt(conf, traceGenBolt + ".parallelism"))
                .directGrouping(imgPrepareBolt, STREAM_EIG_FLOW)
                .allGrouping(traceAggregator, STREAM_INDICATOR_TRACE)
                .setNumTasks(getInt(conf, traceGenBolt + ".tasks"));

        builder.setBolt(optFlowTracker, new optFlowTrackerFox(traceAggregator), getInt(conf, optFlowTracker + ".parallelism"))
                .directGrouping(traceGenBolt, STREAM_NEW_TRACE)
                .directGrouping(traceAggregator, STREAM_RENEW_TRACE)
                .directGrouping(optFlowTrans, STREAM_OPT_FLOW)
                .allGrouping(frameDisplay, STREAM_CACHE_CLEAN)
                .setNumTasks(getInt(conf, optFlowTracker + ".tasks"));

        builder.setBolt(traceAggregator, new traceAggFox(traceGenBolt, optFlowTracker), getInt(conf, traceAggregator + ".parallelism"))
                .directGrouping(traceGenBolt, STREAM_REGISTER_TRACE)
                .directGrouping(optFlowTracker, STREAM_EXIST_REMOVE_TRACE)
                .setNumTasks(getInt(conf, traceAggregator + ".tasks"));

        builder.setBolt(frameDisplay, new frameDisplayMultiGolf(traceAggregator), getInt(conf, frameDisplay + ".parallelism"))
                .fieldsGrouping(imgPrepareBolt, STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(traceAggregator, STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, frameDisplay + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutputFox(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(frameDisplay, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "TrajNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "TrajMaxPending"));

        int min_dis = getInt(conf, "min_distance");
        int init_counter = getInt(conf, "init_counter");
        int inW = ConfigUtil.getInt(conf, "inWidth", 320);
        int inH = ConfigUtil.getInt(conf, "inHeight", 240);
        int orgW = ConfigUtil.getInt(conf, "orgWidth", 640);
        int orgH = ConfigUtil.getInt(conf, "orgHeight", 480);

        conf.registerSerialization(Serializable.Mat.class);
        conf.setStatsSampleRate(1.0);
        StormSubmitter.submitTopology("tTrajTopGolf-" + init_counter + "-" + min_dis + "-" + inW + "-" + inH + "-org-" + orgW + "-" + orgH, conf, topology);
    }
}

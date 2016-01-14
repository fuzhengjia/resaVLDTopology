package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import tool.FrameImplImageSourceFox;
import tool.Serializable;

import java.io.FileNotFoundException;

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Oct 23, 2015
 * 有三个width 和 height， 第一是输入的原始wh， 一个是要缩小到供process的wh，最后是需要在最后输出时的wh
 * 这个版本同时输出traj和action detection结果
 * another implementation for ActDetWithTraj
 * 此版本效果更好
 *
 * 记录于 Dec 15, 2015
 * TODO: action detection的输出window的设计，是否可以做成moving window，每次有新的trajectory到来，就把旧的trajectory去掉
 * TODO: redisFrameOutput的改进问题，priorityQueue和时序管理问题，是否可以放到redisQueue以后的程序做？这样需要在输出到redisQueue的Frame里面带上FrameID的信息
 * TODO: 这样就引出了一个新问题，是否需要重新设计Serializable这个类，让它继承自opencv_core.Mat()同时implement Serializable的一些接口？
 * TODO: FeatureGen和frameDisplayPolingFoxTraj 目前都是globalGrouping，无法实现分布式，是否可以设计成支持分布式？
 */
public class tomTrajDisplayTopFoxActDetWinDrawTraj {

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
        String featurePooling = "featurePooling";
        String featureGenBolt = "featureGen";


        builder.setSpout(spoutName, new FrameImplImageSourceFox(host, port, queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(imgPrepareBolt, new imagePrepareFox(traceGenBolt), getInt(conf, imgPrepareBolt + ".parallelism"))
                .shuffleGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, imgPrepareBolt + ".tasks"));

        builder.setBolt(optFlowGenBolt, new optlFlowGeneratorMultiOptFlowHogMBH(), getInt(conf, optFlowGenBolt + ".parallelism"))
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
                .allGrouping(featureGenBolt, STREAM_CACHE_CLEAN)
                .setNumTasks(getInt(conf, optFlowTracker + ".tasks"));

        builder.setBolt(traceAggregator, new traceAggFoxActDetWithTraj(traceGenBolt, optFlowTracker), getInt(conf, traceAggregator + ".parallelism"))
                .directGrouping(traceGenBolt, STREAM_REGISTER_TRACE)
                .directGrouping(optFlowTracker, STREAM_EXIST_REMOVE_TRACE)
                .setNumTasks(getInt(conf, traceAggregator + ".tasks"));

        builder.setBolt(featureGenBolt, new featureGenFox(traceAggregator), getInt(conf, featureGenBolt + ".parallelism"))
                .globalGrouping(optFlowGenBolt, STREAM_FEATURE_FLOW)
                .globalGrouping(traceAggregator, STREAM_FEATURE_TRACE)
                .setNumTasks(getInt(conf, featureGenBolt + ".tasks"));

        builder.setBolt(featurePooling, new frameDisplayPolingFoxTraj(), getInt(conf, featurePooling + ".parallelism"))
                .globalGrouping(imgPrepareBolt, STREAM_FRAME_OUTPUT)
                .globalGrouping(featureGenBolt, STREAM_FRAME_FV)
                .setNumTasks(getInt(conf, featurePooling + ".tasks"));

        builder.setBolt(frameDisplay, new frameDisplayMultiFox(traceAggregator), getInt(conf, frameDisplay + ".parallelism"))
                .fieldsGrouping(imgPrepareBolt, STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(traceAggregator, STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, frameDisplay + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutputActWithTraj(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(featurePooling, STREAM_FRAME_ACTDET_DISPLAY)
                .globalGrouping(frameDisplay, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "TrajNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "TrajMaxPending"));

        int min_dis = getInt(conf, "min_distance");
        int init_counter = getInt(conf, "init_counter");
        int inW = getInt(conf, "inWidth");
        int inH = getInt(conf, "inHeight");
        int procW = getInt(conf, "procWidth");
        int procH = getInt(conf, "procHeight");
        int outW = getInt(conf, "outputW");
        int outH = getInt(conf, "outputH");
        int drawTrajSampleRate = getInt(conf, "drawTrajSampleRate");
        conf.registerSerialization(Serializable.Mat.class);
        conf.setStatsSampleRate(1.0);

        int frameRate = getInt(conf, "frameRate");
        int windowInSeconds = getInt(conf, "windowInSeconds"); ///windowInFrames = windowInSeconds * frameRate
        int resultLastSeconds = getInt(conf, "resultLastSeconds"); /// Countdown seconds = windowInseconds - resultLastSeconds

        StormSubmitter.submitTopology("tTrajTopFoxActDetWinTrajBeta-" + init_counter + "-" + min_dis + "-" + drawTrajSampleRate
                + "-" + inW + "-" + inH + "-" + procW + "-" + procH+ "-" + outW + "-" + outH
                + "-" + frameRate + "-" + windowInSeconds + "-" + resultLastSeconds, conf, topology);
    }
}

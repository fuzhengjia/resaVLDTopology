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

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Jan 29, 2015.
 * TODO: Notes:
 * traceGenerator 是否可以并行？ 这样需要feedback分开，register也要分开
 * 扩展，如果有2个scale的话，需要对当前程序扩展！
 * 产生光流是bottleneck-> done
 * 此版本暂时通过测试
 * 尝试将optFlowGen and optFlowAgg 分布式化->done
 * 在echo 版本中， optFlowTracker也作了细分，大大降低了传输的network cost
 * 在echo 版本中，ImgPrep在把eig frame传给traceGen的时候，也做了划分，减少了network cost
 * 在echo 版本的基础上，使用batch传输策略，即，将所有的trace的点，收集之后再传给下一个bolt，
 * 这样的batch似乎有效的提高了传输和处理的效率
 * Test 通过，现在18-3能到25fps，具体performance可以查询笔记
 */
public class tomTrajDisplayTopEchoBatch {

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

        builder.setBolt(imgPrepareBolt, new imagePrepareEcho(traceGenBolt), getInt(conf, imgPrepareBolt + ".parallelism"))
                .shuffleGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, imgPrepareBolt + ".tasks"));

        builder.setBolt(optFlowGenBolt, new optlFlowGeneratorMultiOptFlow(), getInt(conf, optFlowGenBolt + ".parallelism"))
                .shuffleGrouping(imgPrepareBolt, STREAM_GREY_FLOW)
                .setNumTasks(getInt(conf, optFlowGenBolt + ".tasks"));

        builder.setBolt(optFlowTrans, new optlFlowTransEcho(optFlowTracker), getInt(conf, optFlowTrans + ".parallelism"))
                .shuffleGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .setNumTasks(getInt(conf, optFlowTrans + ".tasks"));

        builder.setBolt(traceGenBolt, new traceGeneratorEchoBatch(traceAggregator, optFlowTracker), getInt(conf, traceGenBolt + ".parallelism"))
                .directGrouping(imgPrepareBolt, STREAM_EIG_FLOW)
                .allGrouping(traceAggregator, STREAM_INDICATOR_TRACE)
                .setNumTasks(getInt(conf, traceGenBolt + ".tasks"));

        builder.setBolt(optFlowTracker, new optFlowTrackerEchoBatch(traceAggregator), getInt(conf, optFlowTracker + ".parallelism"))
                .directGrouping(traceGenBolt, STREAM_NEW_TRACE)
                .directGrouping(traceAggregator, STREAM_RENEW_TRACE)
                .directGrouping(optFlowTrans, STREAM_OPT_FLOW)
                .allGrouping(frameDisplay, STREAM_CACHE_CLEAN)
                .setNumTasks(getInt(conf, optFlowTracker + ".tasks"));

        builder.setBolt(traceAggregator, new traceAggregatorEchoBatch(traceGenBolt, optFlowTracker), getInt(conf, traceAggregator + ".parallelism"))
                .directGrouping(traceGenBolt, STREAM_REGISTER_TRACE)
                .directGrouping(optFlowTracker, STREAM_EXIST_REMOVE_TRACE)
                .setNumTasks(getInt(conf, traceAggregator + ".tasks"));

        builder.setBolt(frameDisplay, new frameDisplayMultiDelta(traceAggregator), getInt(conf, frameDisplay + ".parallelism"))
                .fieldsGrouping(imgPrepareBolt, STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(traceAggregator, STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, frameDisplay + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutput(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(frameDisplay, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "TrajNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "TrajMaxPending"));

        int min_dis = getInt(conf, "min_distance");
        int init_counter = getInt(conf, "init_counter");
        conf.registerSerialization(Serializable.Mat.class);
        conf.setStatsSampleRate(1.0);
        StormSubmitter.submitTopology("tTrajTopEchoBatch-" + init_counter + "-" + min_dis, conf, topology);
    }
}

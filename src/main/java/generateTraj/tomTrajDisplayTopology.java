package generateTraj;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import showTraj.RedisFrameOutput;
import tool.FrameImplImageSource;
import topology.Serializable;

import java.io.FileNotFoundException;

import static tool.Constant.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Jan 29, 2015.
 */
public class tomTrajDisplayTopology {

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
        String optFlowTracker = "TrajOptFlowTracker";
        String traceAggregator = "TrajTraceAgg";
        String frameDisplay = "TrajDisplay";

        builder.setSpout(spoutName, new FrameImplImageSource(host, port, queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(imgPrepareBolt, new imagePrepare(), getInt(conf, imgPrepareBolt + ".parallelism"))
                .globalGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getInt(conf, imgPrepareBolt + ".tasks"));

        builder.setBolt(optFlowGenBolt, new optlFlowGenerator(), getInt(conf, optFlowGenBolt + ".parallelism"))
                .globalGrouping(imgPrepareBolt, STREAM_GREY_FLOW)
                .setNumTasks(getInt(conf, optFlowGenBolt + ".tasks"));

        builder.setBolt(traceGenBolt, new traceGenerator(), getInt(conf, traceGenBolt + ".parallelism"))
                .shuffleGrouping(imgPrepareBolt, STREAM_NEW_TRACE)
                //.fieldsGrouping(imgPrepareBolt, STREAM_NEW_TRACE, new Fields(FIELD_TRACE_POINT))
                .allGrouping(optFlowTracker, STREAM_RENEW_TRACE)
                .setNumTasks(getInt(conf, traceGenBolt + ".tasks"));

        builder.setBolt(optFlowTracker, new optFlowTracker(), getInt(conf, optFlowTracker + ".parallelism"))
                .shuffleGrouping(traceGenBolt, STREAM_EXIST_TRACE)
                .allGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .allGrouping(traceAggregator, STREAM_CACHE_CLEAN)
                .setNumTasks(getInt(conf, optFlowTracker + ".tasks"));

        builder.setBolt(traceAggregator, new traceAggregator(), getInt(conf, traceAggregator + ".parallelism"))
                .globalGrouping(traceGenBolt, STREAM_REGISTER_TRACE)
                .globalGrouping(optFlowTracker, STREAM_EXIST_TRACE)
                .globalGrouping(optFlowTracker, STREAM_RENEW_TRACE)
                .globalGrouping(optFlowTracker, STREAM_REMOVE_TRACE)
                .setNumTasks(getInt(conf, traceAggregator + ".tasks"));

        builder.setBolt(frameDisplay, new frameDisplay(), getInt(conf, frameDisplay + ".parallelism"))
                .globalGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .globalGrouping(traceAggregator, STREAM_PLOT_TRACE)
                .setNumTasks(getInt(conf, frameDisplay + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "TrajNumOfWorkers");
        //int numberOfAckers = getInt(conf, "numberOfAckers");
        conf.setNumWorkers(numberOfWorkers);
        //conf.setNumAckers(numberOfAckers);
        conf.setMaxSpoutPending(getInt(conf, "TrajMaxPending"));

        conf.registerSerialization(Serializable.Mat.class);
        StormSubmitter.submitTopology("tomTrajDisplayTopology", conf, topology);

    }
}

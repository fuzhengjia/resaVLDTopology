package vldTest;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.io.FileNotFoundException;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;
import static topology.StormConfigManager.readConfig;

/**
 * Created by Tom
 * Difference to previous version:
 * 1. use new VideoSender module,
 * 2. use new videoProducer module,
 * 3. Remove the Cache_clear_stream -> failed, poorer performance!
 */
public class testVLDTopology {


    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);
        int numberOfWorkers = getInt(conf, "numberOfWorkers");

        TopologyBuilder builder = new TopologyBuilder();
        String host = getString(conf, "redis.host");
        int port = getInt(conf, "redis.port");
        String queueName = getString(conf, "redis.sourceQueueName");

        builder.setSpout("t-FSout", new testFrameSource(host, port, queueName), getInt(conf, "TomFrameSpout.parallelism"))
                .setNumTasks(getInt(conf, "TomFrameSpout.tasks"));

        builder.setBolt("t-patchGen", new testPatchGenBolt("t-processor"), getInt(conf, "TomPatchGen.parallelism"))
                .allGrouping("t-FSout", RAW_FRAME_STREAM)
                //.allGrouping("t-intermediate", CACHE_CLEAR_STREAM)
                //.allGrouping("t-processor", LOGO_TEMPLATE_UPDATE_STREAM)
                .setNumTasks(getInt(conf, "TomPatchGen.tasks"));

        builder.setBolt("t-processor", new testPatchProcBolt(), getInt(conf, "PatchProcessorBolt.parallelism"))
                .shuffleGrouping("t-patchGen", PATCH_STREAM)
                .allGrouping("t-processor", LOGO_TEMPLATE_UPDATE_STREAM)
                .directGrouping("t-patchGen", RAW_FRAME_STREAM)
                //.directGrouping("t-patchGen", CACHE_CLEAR_STREAM)
                //.directGrouping("t-patchGen", LOGO_TEMPLATE_UPDATE_STREAM)
                .allGrouping("t-intermediate", CACHE_CLEAR_STREAM)
                .setNumTasks(getInt(conf, "PatchProcessorBolt.tasks"));

        builder.setBolt("t-intermediate", new testPatchAggBolt(), getInt(conf, "PatchAggregatorBolt.parallelism"))
                .fieldsGrouping("t-processor", DETECTED_LOGO_STREAM, new Fields("frameId"))
                .setNumTasks(getInt(conf, "PatchAggregatorBolt.tasks"));

        builder.setBolt("t-aggregator", new testRedisFrameAggBolt(), getInt(conf, "FrameAggregatorBolt.parallelism"))
                .globalGrouping("t-intermediate", PROCESSED_FRAME_STREAM)
                .globalGrouping("t-FSout", RAW_FRAME_STREAM)
                .setNumTasks(getInt(conf, "FrameAggregatorBolt.tasks"));

        StormTopology topology = builder.createTopology();

        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "MaxSpoutPending"));

        StormSubmitter.submitTopology("testVLDTop", conf, topology);

    }
}

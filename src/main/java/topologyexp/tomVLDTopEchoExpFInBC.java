package topologyexp;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import showTraj.RedisFrameOutput;
import topology.tDrawPatchDelta;
import topology.tomFrameSpoutResize;
import util.ConfigUtil;

import java.io.FileNotFoundException;
import java.util.List;

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu, this version is through basic testing.
 *
 * This is for experiment purpose  (results for paper)
 * This is use the very original version by Nurlan,
 * the spout (or patchGen) broadcasts the raw frames to all the patchProcessingBolt, and shuffles those patch identifiers.
 *
 * Updated on April 29, the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 */
public class tomVLDTopEchoExpFInBC {

    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);

        TopologyBuilder builder = new TopologyBuilder();
        String spoutName = "tVLDSpout";
        String patchGenBolt = "tVLDPatchGen";
        String patchProcBolt = "tVLDPatchProc";
        String patchAggBolt = "tVLDPatchAgg";
        String patchDrawBolt = "tVLDPatchDraw";
        String redisFrameOut = "tVLDRedisFrameOut";

        //todo: still have problem when sample rate is even number!!!

        builder.setSpout(spoutName, new tomFrameSpoutResize(), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(patchGenBolt, new tomPatchGenWSampleBC(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, SAMPLE_FRAME_STREAM) //TODO: double check the new sampling handling approach.
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(patchProcBolt, new PatchProcessorBoltMultipleEcho(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .allGrouping(patchAggBolt, CACHE_CLEAR_STREAM)
                .shuffleGrouping(patchGenBolt, PATCH_STREAM)
                .allGrouping(patchGenBolt, RAW_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(patchAggBolt, new PatchAggBoltMultipleBeta(), getInt(conf, patchAggBolt + ".parallelism"))
                .globalGrouping(patchProcBolt, DETECTED_LOGO_STREAM)//todo: double check, make sure the output of last bolt has FrameID field!!
                //todo: still have problem when sample rate is even number!!!
                //.fieldsGrouping(patchProcBolt, DETECTED_LOGO_STREAM, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, patchAggBolt + ".tasks"));

        builder.setBolt(patchDrawBolt, new tDrawPatchDelta(), getInt(conf, patchDrawBolt + ".parallelism"))
                .fieldsGrouping(patchAggBolt, PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(spoutName, RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, patchDrawBolt + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutput(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(patchDrawBolt, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "tVLDNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "tVLDMaxPending"));

        conf.setStatsSampleRate(1.0);
        //conf.registerSerialization(Serializable.Mat.class);
        int sampleFrames = getInt(conf, "sampleFrames");
        int W = ConfigUtil.getInt(conf, "width", 640);
        int H = ConfigUtil.getInt(conf, "height", 480);

        List<String> templateFiles = getListOfStrings(conf, "originalTemplateFileNames");

        StormSubmitter.submitTopology("tomVLDTopEchoExpFInBC-s" + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size(), conf, topology);

    }
}

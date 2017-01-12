package topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import tool.FrameSourceFoxFromCamera;
import tool.RedisFrameOutputFox;
import tool.Serializable;

import java.io.FileNotFoundException;
import java.util.List;

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu, a new version based on echoOneGenRIRO!!
 * In the delta version, we enables the feature of supporting the multiple logo input,
 * When the setting in the configuration file includes multiple logo image files,
 * it automatically creates corresponding detector instance
 * Note: we in this version's patchProc bolt (tPatchProcessorDelta), uses the StormVideoLogoDetectorGamma
 * This gamma Detector helps to decrease overhead of multiple logo image files.
 *
 * Through testing, when sampleFrame = 4, it supports up to 25 fps.
 * Updated on April 29, the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 *
 * Enabling sampling features. Sampling problem is solved!
 * through testing
 *
 * Write on Dec 15, 2015
 * TODO: some new improvement point: 1. re-design of redisFrameOutput, the sorting queue can be moved out to the explicit programme.
 * TODO: the output can be frameID + frame to the redis queue, (need to modify serializableMat), so that when the explicit programme pop from redis queue, it can do the sorting
 * TODO: a question? can we re-write serializableMat, which is extended from opencv_core.Mat, implements io.serializable and kysto?
 */
public class VLDTopFoxFromCamera {


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

        builder.setSpout(spoutName, new FrameSourceFoxFromCamera(), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(patchGenBolt, new PatchGenFox(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, SAMPLE_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(patchProcBolt, new PatchProcessorFox(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .shuffleGrouping(patchGenBolt, PATCH_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(patchAggBolt, new PatchAggFox(), getInt(conf, patchAggBolt + ".parallelism"))
                .fieldsGrouping(patchProcBolt, DETECTED_LOGO_STREAM, new Fields(FIELD_SAMPLE_ID))
                .setNumTasks(getInt(conf, patchAggBolt + ".tasks"));

        builder.setBolt(patchDrawBolt, new tDrawPatchDelta(), getInt(conf, patchDrawBolt + ".parallelism"))
                .fieldsGrouping(patchAggBolt, PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(spoutName, RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, patchDrawBolt + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutputFox(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(patchDrawBolt, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "tVLDNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "tVLDMaxPending"));

        conf.setStatsSampleRate(1.0);
        conf.registerSerialization(Serializable.Mat.class);
        int sampleFrames = getInt(conf, "sampleFrames");
        int W = getInt(conf, "rtsp.camera.out.width");
        int H = getInt(conf, "rtsp.camera.out.height");

        List<String> templateFiles = getListOfStrings(conf, "originalTemplateFileNames");

        //StormSubmitter.submitTopology("VLDTopFoxFromCamera-s" + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size(), conf, topology);
        System.out.println("submitting topology: " + "VLDTopFoxFromCamera-s" + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size());
        StormSubmitter.submitTopology("VLDTopFoxFromCamera", conf, topology);
    }
}

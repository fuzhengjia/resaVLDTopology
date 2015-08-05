package topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import logodetection.Debug;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_features2d;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import tool.Serializable;

import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;


/**
 * Created by Intern04 on 4/8/2014.
 */
public class FrameRetrieverSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    private String SOURCE_FILE;
    private FFmpegFrameGrabber grabber;
    private int frameId;
    private long lastFrameTime;
    private int delayInMS;

    int firstFrameId;
    int lastFrameId;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        frameId = 0;
        firstFrameId = getInt(map, "firstFrameId");
        lastFrameId = getInt(map, "lastFrameId");
        SOURCE_FILE = getString(map, "videoSourceFile");
        grabber = new FFmpegFrameGrabber(SOURCE_FILE);
        opencv_features2d.KeyPoint kp = new opencv_features2d.KeyPoint();
        System.out.println("Created capture: " + SOURCE_FILE);

        delayInMS = getInt(map, "inputFrameDelay");

        this.collector = spoutOutputCollector;
        try {
            grabber.start();
            while (++frameId < firstFrameId)
                grabber.grab();

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }

        kp.deallocate();

        if (Debug.topologyDebugOutput)
            System.out.println("Grabber started");


        if (Debug.timer)
            System.out.println("TIME=" + System.currentTimeMillis());

    }

    opencv_core.IplImage image;
    opencv_core.Mat mat;

    @Override
    public void nextTuple() {
        long now = System.currentTimeMillis();
        if (now - lastFrameTime < delayInMS) {
            return;
        } else {
            lastFrameTime = now;
        }

        if (frameId < lastFrameId) {
            try {
                long start = System.currentTimeMillis();
                image = grabber.grab();
                mat = new opencv_core.Mat(image);

                Serializable.Mat sMat = new Serializable.Mat(mat);

                //TODO get params from config map
                double fx = .25, fy = .25;
                double fsx = .5, fsy = .5;

                int W = sMat.getCols(), H = sMat.getRows();
                int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
                int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
                int patchCount = 0;
                for (int x = 0; x + w <= W; x += dx)
                    for (int y = 0; y + h <= H; y += dy)
                        patchCount++;

                collector.emit(RAW_FRAME_STREAM, new Values(frameId, sMat, patchCount), frameId);
                for (int x = 0; x + w <= W; x += dx) {
                    for (int y = 0; y + h <= H; y += dy) {
                        Serializable.PatchIdentifier identifier = new
                                Serializable.PatchIdentifier(frameId, new Serializable.Rect(x, y, w, h));
                        collector.emit(PATCH_STREAM, new Values(identifier, patchCount), identifier.toString());
                    }
                }
                frameId++;
                long nowTime = System.currentTimeMillis();
                System.out.printf("Sendout: " + nowTime + "," + frameId + ",used: " + (nowTime -start));
            } catch (FrameGrabber.Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_STREAM, new Fields(FIELD_PATCH_IDENTIFIER, FIELD_PATCH_COUNT));
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_PATCH_COUNT));
    }


}

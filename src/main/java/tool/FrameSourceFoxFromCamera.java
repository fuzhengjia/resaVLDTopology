package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import util.ConfigUtil;

import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Updated on Aug 5,  the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 * also binding to ImageSenderFox!
 */
public class FrameSourceFoxFromCamera extends BaseRichSpout {

    private int frameId;
    private int sampleID;
    private int sampleFrames;

    protected SpoutOutputCollector collector;

    private String videoSourceStream;
    private int fps;
    private int targetH;
    private int targetW;
    private FFmpegFrameGrabber streamGrabber;

    public FrameSourceFoxFromCamera() {
    }

    private long startTime;
    private long lastTime;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        frameId = 0;
        sampleID = 0;
        sampleFrames = ConfigUtil.getInt(conf, "sampleFrames", 1);

        fps = getInt(conf, "rtsp.camera.out.fps");
        videoSourceStream = getString(conf, "rtsp.camera.out.sourceStream");
        targetW = getInt(conf, "rtsp.camera.out.width");
        targetH = getInt(conf, "rtsp.camera.out.height");

        streamGrabber = new FFmpegFrameGrabber(videoSourceStream);
        streamGrabber.setFrameRate(fps);
        if (targetW > 0 && targetH > 0) {
            streamGrabber.setImageWidth(targetW);
            streamGrabber.setImageHeight(targetH);
        }

        try {
            streamGrabber.start();
            Thread.sleep(1000);

            startTime = System.currentTimeMillis();
            lastTime = startTime;

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String id = String.valueOf(frameId);

        opencv_core.IplImage fkImage = new opencv_core.IplImage();

        try {
            opencv_core.IplImage image = streamGrabber.grab();
            opencv_core.Mat mat = new opencv_core.Mat(image);
            Serializable.Mat sMat = new Serializable.Mat(mat);

            collector.emit(RAW_FRAME_STREAM, new Values(frameId, sMat), id);
            if (frameId % sampleFrames == 0) {
                collector.emit(SAMPLE_FRAME_STREAM, new Values(frameId, sMat, sampleID), frameId);
                sampleID++;
            }
//            long nowTime = System.currentTimeMillis();
//            System.out.printf("Sendout: " + nowTime + "," + frameId);
            frameId++;

            int remainCnt = frameId % fps;
            if (remainCnt == 0) {
                long current = System.currentTimeMillis();
                long elapse = current - lastTime;
                long remain = 1000 - elapse;
                lastTime = current;
                System.out.println("Current: " + current + ", elapsed: " + (lastTime - startTime)
                        + ",totalSend: " + frameId + ", remain: " + remain + ", W: " + mat.cols() + ", H: " + mat.rows());
            }

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        declarer.declareStream(SAMPLE_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_SAMPLE_ID));
    }
}

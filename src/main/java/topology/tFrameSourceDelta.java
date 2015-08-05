package topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.RedisQueueSpout;
import tool.Serializable;
import util.ConfigUtil;

import java.util.Map;

import static tool.Constants.*;

/**
 * Updated on April 29, the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 */
public class tFrameSourceDelta extends RedisQueueSpout {

    private int frameId;
    private int sampleFrames;

    int W,H;

    public tFrameSourceDelta(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        sampleFrames = ConfigUtil.getInt(conf, "sampleFrames", 1);
        //opencv_core.IplImage fk = new opencv_core.IplImage();

        W = ConfigUtil.getInt(conf, "width", 640);
        H = ConfigUtil.getInt(conf, "height", 480);
    }

    @Override
    protected void emitData(Object data) {
        //String id = idPrefix + frameId++;
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;
        opencv_core.IplImage image = opencv_highgui.cvDecodeImage(opencv_core.cvMat(1, imgBytes.length, opencv_core.CV_8UC1, new BytePointer(imgBytes)));

        opencv_core.Mat mat = new opencv_core.Mat(image);
        opencv_core.Mat matNew = new opencv_core.Mat();
        opencv_core.Size size = new opencv_core.Size(W, H);
        opencv_imgproc.resize(mat, matNew, size);

        //Serializable.Mat sMat = new Serializable.Mat(mat);
        Serializable.Mat sMat = new Serializable.Mat(matNew);

        collector.emit(RAW_FRAME_STREAM, new Values(frameId, sMat), id);
        //if (frameId % sampleFrames == 0) {
        //    collector.emit(SAMPLE_FRAME_STREAM, new Values(frameId, sMat), frameId);
        //}
        long nowTime = System.currentTimeMillis();
        System.out.printf("Sendout: " + nowTime + "," + frameId);
        frameId ++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        //declarer.declareStream(SAMPLE_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

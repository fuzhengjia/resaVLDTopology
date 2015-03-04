package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import topology.Serializable;
import util.ConfigUtil;

import javax.imageio.stream.ImageInputStream;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static tool.Constant.*;

/**
 * Created by Tom Fu on Jan 28, 2015
 * This version is an alternative implementation, which every time emit two consecutive raw frames,
 * it ease the processing of imagePrepare and opticalFlow calculation, at the cost of more network transmissions
 */
public class FrameOptFlowSource extends RedisQueueSpout {

    private int frameId;
    private Serializable.Mat sMatPrev;

    public FrameOptFlowSource(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 1;
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    protected void emitData(Object data) {
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;
        ImageInputStream iis = null;

        try {

            Serializable.Mat[] sMats = Serializable.Mat.toSMat(imgBytes);
            Serializable.Mat rawMat = sMats[0];
            Serializable.Mat optMat = sMats[1];

            collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, rawMat), id);
            collector.emit(STREAM_OPT_FLOW, new Values(frameId, optMat), id);

            long nowTime = System.currentTimeMillis();
            System.out.printf("Sendout: " + nowTime + "," + frameId);
            frameId++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        declarer.declareStream(STREAM_OPT_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

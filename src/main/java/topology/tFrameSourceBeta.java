package topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import tool.RedisQueueSpout;
import tool.Serializable;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.cvMat;
import static tool.Constants.*;

/**
 * Created by ding on 14-7-3.
 */
public class tFrameSourceBeta extends RedisQueueSpout {

    private int frameId;
    //private String idPrefix;

    public tFrameSourceBeta(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        //opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    protected void emitData(Object data) {
        //String id = idPrefix + frameId++;
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;
        opencv_core.IplImage image = opencv_highgui.cvDecodeImage(opencv_core.cvMat(1, imgBytes.length, opencv_core.CV_8UC1, new BytePointer(imgBytes)));

        opencv_core.Mat mat = new opencv_core.Mat(image);
        Serializable.Mat sMat = new Serializable.Mat(mat);

        collector.emit(RAW_FRAME_STREAM, new Values(frameId, sMat), id);

        long nowTime = System.currentTimeMillis();
        System.out.printf("Sendout: " + nowTime + "," + frameId);
        frameId ++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

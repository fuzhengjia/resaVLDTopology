package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_imgproc;
import util.ConfigUtil;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu on Aug 3,2015
 * This version is an alternative implementation, which every time emit two consecutive raw frames,
 *  it ease the processing of imagePrepare and opticalFlow calculation, at the cost of more network transmissions
 *  Special for fox version. be careful to use SimpleImageSenderFox version!!!
 *  In summary, from imageSender (IplImage->Mat->sMat->byte[]) to redis queue -> byte[]->sMat->Mat->IplImage)
 *
 * Golf version 是基于Fox version， 实现一个新功能，按照Peiyong的建议，当输入文件的resolution很大的时候，可以先resize变小，产生trajectory之后，
 * 再显示的时候resize回来。
 * There shall be two w-h information, the original w-h (orgWid, orgHei), and the processing w-h (smaller, inWid, inHei).
 */
public class FrameImplImageSourceGolf extends RedisQueueSpout {

    private int frameId;
    private int nChannel;
    private int nDepth;
    private int inHeight;
    private int inWidth;

    private Serializable.Mat sMatPrev;

    private boolean toDebug = false;

    public FrameImplImageSourceGolf(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        nChannel = ConfigUtil.getInt(conf, "nChannel", 3);
        nDepth = ConfigUtil.getInt(conf, "nDepth", 8);
        inWidth = ConfigUtil.getInt(conf, "inWidth", 320);
        inHeight = ConfigUtil.getInt(conf, "inHeight", 240);

        toDebug = ConfigUtil.getBoolean(conf, "debugTopology", false);

        sMatPrev = null;
    }

    @Override
    protected void emitData(Object data) {
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;

        try {
            IplImage fkImage = new IplImage();
            Serializable.Mat revMat = new Serializable.Mat(imgBytes);

            IplImage image = revMat.toJavaCVMat().asIplImage();
            IplImage frame = cvCreateImage(cvSize(inWidth, inHeight), nDepth, nChannel);
            opencv_imgproc.cvResize(image, frame, opencv_imgproc.CV_INTER_AREA);

            Mat mat = new Mat(frame);
            Serializable.Mat sMat = new Serializable.Mat(mat);

            if (frameId > 0 && sMatPrev != null){
                collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, sMat, sMatPrev), id);
                collector.emit(ORIGINAL_FRAME_OUTPUT, new Values(frameId, revMat), id);

                if (toDebug) {
                    long nowTime = System.currentTimeMillis();
                    System.out.printf("Sendout: " + nowTime + "," + frameId);
                }
            }
            frameId++;
            sMatPrev = new Serializable.Mat(mat);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_FRAME_MAT_PREV));
        declarer.declareStream(ORIGINAL_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

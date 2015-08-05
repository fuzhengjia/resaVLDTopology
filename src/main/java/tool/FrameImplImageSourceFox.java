package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
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
 */
public class FrameImplImageSourceFox extends RedisQueueSpout {

    private int frameId;
    private int nChannel;
    private int nDepth;
    private int inHeight;
    private int inWidth;

    private Serializable.Mat sMatPrev;

    private boolean toDebug = false;

    public FrameImplImageSourceFox(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        nChannel = ConfigUtil.getInt(conf, "nChannel", 3);
        nDepth = ConfigUtil.getInt(conf, "nDepth", 8);
        inWidth = ConfigUtil.getInt(conf, "inWidth", 640);
        inHeight = ConfigUtil.getInt(conf, "inHeight", 480);

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

            opencv_core.Mat mat = new opencv_core.Mat(frame);
            Serializable.Mat sMat = new Serializable.Mat(mat);

            if (frameId > 0 && sMatPrev != null){
                collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, sMat, sMatPrev), id);

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
    }
}

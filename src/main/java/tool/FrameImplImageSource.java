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

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static tool.Constant.*;

/**
 * Created by Tom Fu on Jan 28, 2015
 */
public class FrameImplImageSource extends RedisQueueSpout {

    private int frameId;
    //private String idPrefix;
    private int nChannel;
    private int nDepth;
    private int inHeight;
    private int inWidth;

    public FrameImplImageSource(String host, int port, String queue) {
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
    }

    @Override
    protected void emitData(Object data) {
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;
        ImageInputStream iis = null;

        try {
            iis = ImageIO.createImageInputStream(new ByteArrayInputStream(imgBytes));
            BufferedImage img = ImageIO.read(iis);
            opencv_core.IplImage image = opencv_core.IplImage.createFrom(img);

            //byte[] imgBytes = (byte[]) tuple.getValueByField(FIELD_FRAME_BYTES);
            //opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));

            opencv_core.IplImage frame = cvCreateImage(cvSize(inWidth, inHeight), nDepth, nChannel);
            opencv_imgproc.cvResize(image, frame, opencv_imgproc.CV_INTER_AREA);

            opencv_core.Mat mat = new opencv_core.Mat(image);
            Serializable.Mat sMat = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, sMat), id);

            long nowTime = System.currentTimeMillis();
            System.out.printf("Sendout: " + nowTime + "," + frameId);
            frameId++;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

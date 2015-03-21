package vldTest;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import tool.RedisQueueSpout;
import topology.Serializable;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.util.Map;

import static tool.Constants.RAW_FRAME_STREAM;


/**
 * Created by ding on 14-7-3.
 */
public class testFrameSource extends RedisQueueSpout {

    private int frameId;
    //private String idPrefix;
    public testFrameSource(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        //idPrefix = String.format("s-%02d-", context.getThisTaskIndex() + 1);
    }


    @Override
    protected void emitData(Object data) {
        //String id = idPrefix + frameId++;
        try {
            String id = String.valueOf(frameId);

            byte[] imgBytes = (byte[]) data;

            ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(imgBytes));
            BufferedImage img = ImageIO.read(iis);

            opencv_core.IplImage image = opencv_core.IplImage.createFrom(img);


            //opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));

            opencv_core.Mat mat = new opencv_core.Mat(image);
            Serializable.Mat sMat = new Serializable.Mat(mat);

            collector.emit(RAW_FRAME_STREAM, new Values(frameId, sMat, 0), id);

            long nowTime = System.currentTimeMillis();
            System.out.printf("Sendout: " + nowTime + "," + frameId);
            frameId++;
        }catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RAW_FRAME_STREAM, new Fields("frameId", "frameMat", "patchCount"));
    }
}

package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import showTraj.RedisStreamProducer;
import topology.Serializable;
import topology.StreamFrame;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.cvMat;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static util.ConfigUtil.*;
import static tool.Constant.*;

import org.bytedeco.javacpp.opencv_core;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameOutput extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducer2 producer;

    private String host;
    private int port;
    private String queueName;
    private int accumulateFrameSize;
    private int sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.host = (String) map.get("redis.host");
        this.port = getInt(map, "redis.port", 6379);
        this.queueName = (String) map.get("redis.queueName");

        this.accumulateFrameSize = getInt(map, "accumulateFrameSize", 1);
        this.sleepTime = getInt(map, "sleepTime", 10);
        this.startFrameID = getInt(map, "startFrameID", 1);
        this.maxWaitCount = getInt(map, "maxWaitCount", 4);

        producer = new RedisStreamProducer2(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        //byte[] imgBytes = sMat.toString().getBytes();
        opencv_core.IplImage image = new opencv_core.IplImage();
        //opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));
        //opencv_core.Mat mat = sMat.toJavaCVMat();
        producer.addFrame(new StreamFrame(frameId, sMat.toJavaCVMat()));

        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

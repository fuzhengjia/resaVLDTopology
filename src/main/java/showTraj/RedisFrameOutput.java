package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import tool.RedisStreamProducerBeta;
import topology.RedisStreamProducer;
import topology.Serializable;
import topology.StreamFrame;

import java.util.HashMap;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.cvMat;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static util.ConfigUtil.*;
import static tool.Constant.*;

import org.bytedeco.javacpp.opencv_core;
import util.ConfigUtil;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameOutput extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducerBeta producer;
    //RedisStreamProducer producer;

    private String host;
    private int port;
    private String queueName;
    private int sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    //private int accumulateFrameSize;

    //private HashMap<Integer, Serializable.Mat> rawFrameMap;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.host = (String) map.get("redis.host");
        this.port = getInt(map, "redis.port", 6379);
        this.queueName = (String) map.get("redis.queueName");

        this.sleepTime = getInt(map, "sleepTime", 10);
        this.startFrameID = getInt(map, "startFrameID", 1);
        this.maxWaitCount = getInt(map, "maxWaitCount", 4);

//        accumulateFrameSize = ConfigUtil.getInt(map, "accumulateFrameSize", 1);
//        rawFrameMap = new HashMap<>();

        producer = new RedisStreamProducerBeta(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        //producer = new RedisStreamProducer(host, port, queueName, accumulateFrameSize);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

//        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
//        rawFrameMap.computeIfAbsent(frameId, k->sMat);
//
//        opencv_core.Mat orgMat = rawFrameMap.get(frameId).toJavaCVMat();
//        opencv_core.IplImage frame = orgMat.asIplImage();
//
//        opencv_core.Mat mat = new opencv_core.Mat(frame);
//        producer.addFrame(new StreamFrame(frameId, mat));

        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        producer.addFrame(new StreamFrame(frameId, sMat.toJavaCVMat()));

//        rawFrameMap.remove(frameId);
        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

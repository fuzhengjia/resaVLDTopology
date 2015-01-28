package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;
import tool.RedisStreamProducerBeta;
import topology.Serializable;
import topology.StreamFrame;

import java.util.Map;

import static tool.Constant.FIELD_FRAME_ID;
import static tool.Constant.FIELD_FRAME_IMPL;
import static util.ConfigUtil.getInt;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisImageFrameOutput extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducerBeta producer;

    private String host;
    private int port;
    private String queueName;
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

        this.sleepTime = getInt(map, "sleepTime", 10);
        this.startFrameID = getInt(map, "startFrameID", 1);
        this.maxWaitCount = getInt(map, "maxWaitCount", 4);

        producer = new RedisStreamProducerBeta(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.IplImage sImage = (Serializable.IplImage) tuple.getValueByField(FIELD_FRAME_IMPL);
        opencv_core.Mat mat = new opencv_core.Mat(sImage.createJavaIplImage());

        producer.addFrame(new StreamFrame(frameId, mat));

        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

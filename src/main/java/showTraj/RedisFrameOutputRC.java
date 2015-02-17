package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;
import tool.RedisStreamProducerBeta;
import topology.Serializable;
import topology.StormConfigManager;
import topology.StreamFrame;

import java.util.Map;

import static tool.Constant.*;
import static util.ConfigUtil.getInt;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameOutputRC extends BaseRichBolt {
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

    private int recentFinishedFrameID;
    private int maxPending;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.host = (String) map.get("redis.host");
        this.port = getInt(map, "redis.port", 6379);
        this.queueName = (String) map.get("redis.queueName");

        this.sleepTime = getInt(map, "sleepTime", 10);
        this.startFrameID = getInt(map, "startFrameID", 1);
        this.maxWaitCount = getInt(map, "maxWaitCount", 4);

        this.recentFinishedFrameID = 0;
        this.maxPending = getInt(map, "RCMaxPending", 10);

//        accumulateFrameSize = ConfigUtil.getInt(map, "accumulateFrameSize", 1);
//        rawFrameMap = new HashMap<>();

        producer = new RedisStreamProducerBeta(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        //producer = new RedisStreamProducer(host, port, queueName, accumulateFrameSize);
        new Thread(producer).start();

    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);

        if (streamId.equals(STREAM_FRAME_OUTPUT)) {
            if (frameId - recentFinishedFrameID > maxPending) {
                collector.emit(STREAM_FRAME_OUTPUT, tuple, new Values(frameId, sMat));
                System.out.println("RC on frame: " + frameId + ", recentFinished: " + recentFinishedFrameID + ", at: " + System.currentTimeMillis());
            }
        }else if (streamId.equals(STREAM_FRAME_DISPLAY)){
            producer.addFrame(new StreamFrame(frameId, sMat.toJavaCVMat()));
            System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;
import topology.RedisStreamProducer;
import topology.Serializable;
import topology.StreamFrame;
import util.ConfigUtil;

import java.util.Map;

import static tool.Constants.FIELD_FRAME_ID;
import static tool.Constants.FIELD_FRAME_MAT;
import static util.ConfigUtil.getInt;

/**
 * Created by Tom Fu
 *
 * This is usually the last bolt for displaying or outputting finished frames
 *
 * In the current implementation, it uses RedisStreamProducer instance for outputting as jpg figures to redis output queue
 * RedisStreamProducer maintains a priority queue for each input frame according to their frameID, the smaller the higher priority
 *
 * it maintains a fixed queue length, when a new frame comes, the frame with the smallest frameID in the queue will be popped up and displayed.
 *
 * The alternation can be RedisStreamProducerBeta
 */
public class RedisFrameOutputAlpha extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducer producer;

    private String host;
    private int port;
    private String queueName;

    private int accumulateFrameSize;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.host = (String) map.get("redis.host");
        this.port = getInt(map, "redis.port", 6379);
        this.queueName = (String) map.get("redis.queueName");

        accumulateFrameSize = ConfigUtil.getInt(map, "accumulateFrameSize", 1);

        producer = new RedisStreamProducer(host, port, queueName, accumulateFrameSize);
        new Thread(producer).start();
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        producer.addFrame(new StreamFrame(frameId, sMat.toJavaCVMat()));

        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import tool.RedisStreamObjectProducerByteArr;
import tool.StreamObject;

import java.util.Map;

import static tool.Constants.FIELD_FRAME_ID;
import static tool.Constants.FIELD_FRAME_MAT;
import static util.ConfigUtil.getInt;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameOutputByteArr extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamObjectProducerByteArr producer;

    private String host;
    private int port;
    private String queueName;
    private int sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.host = (String) map.get("redis.host");
        this.port = getInt(map, "redis.port", 6379);
        this.queueName = (String) map.get("redis.ofq");

        this.sleepTime = getInt(map, "of-sleepTime", 10);
        this.startFrameID = getInt(map, "of-startFrameID", 1);
        this.maxWaitCount = getInt(map, "of-maxWaitCount", 4);

        producer = new RedisStreamObjectProducerByteArr(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        new Thread(producer).start();
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        producer.addFrame(new StreamObject(frameId, tuple.getValueByField(FIELD_FRAME_MAT)));

        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

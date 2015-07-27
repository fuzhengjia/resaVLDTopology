package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import tool.GeneralizedStreamFrame;
import tool.RedisSimpleStreamProducer;

import java.util.List;
import java.util.Map;

import static tool.Constants.FIELD_FEA_VEC;
import static tool.Constants.FIELD_FRAME_ID;
import static util.ConfigUtil.getInt;

/**
 * Created by Tom Fu
 *
 * This is usually the last bolt for displaying or outputting finished frames
 * In the current implementation, it uses RedisStreamProducerBeta instance for outputting as jpg figures to redis output queue
 * RedisStreamProducerBeta keeps a timer for each frame, if the expected frame is late, it starts the time and wait until timeout,
 * then it simply drops this frame and come to the next expected frame. (suitable for loss insensitive application)
 *
 * The alternation can be RedisStreamProducer, which maintains a fix length sorted queue as a buffer for sorting, and re-ordering
 * dump out the data at head only when new data arrive at the tail.
 */
public class RedisSimpleFrameOutput extends BaseRichBolt {
    OutputCollector collector;

    RedisSimpleStreamProducer producer;

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

        producer = new RedisSimpleStreamProducer(host, port, queueName);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        List<float[]> data = (List<float[]>)tuple.getValueByField(FIELD_FEA_VEC);

        producer.addFrame(new GeneralizedStreamFrame(frameId, data));

        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

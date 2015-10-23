package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.RedisStreamProducerFox;
import tool.Serializable;
import topology.StreamFrame;
import util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;

import static tool.Constants.*;
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
 *
 * Caution!!  RedisStreamProducerFox is used!
 * Shall use TomVideoStreamReceiverByteArrForLinux for the output!!
 *
 * This is specially designed for action detection and show traj
 */
public class RedisFrameOutputActWithTraj extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducerFox producer;

    private String host;
    private int port;
    private String queueName;
    private int sleepTime;
    private int startFrameID;
    private int maxWaitCount;
    private boolean toDebug = false;


    private HashMap<Integer, Serializable.Mat> trajFrameMap;
    private HashMap<Integer, Serializable.Mat> actFrameMap;

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
        toDebug = ConfigUtil.getBoolean(map, "debugTopology", false);

        trajFrameMap = new HashMap<>();
        actFrameMap = new HashMap<>();

        producer = new RedisStreamProducerFox(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        new Thread(producer).start();

    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        if (streamId.equals(STREAM_FRAME_DISPLAY)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            trajFrameMap.computeIfAbsent(frameId, k -> sMat);
        } else if (streamId.equals(STREAM_FRAME_ACTDET_DISPLAY)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            actFrameMap.computeIfAbsent(frameId, k -> sMat);
        }

        if (trajFrameMap.containsKey(frameId) && actFrameMap.containsKey(frameId)){
            opencv_core.Mat trajMat = trajFrameMap.get(frameId).toJavaCVMat();
            opencv_core.Mat actMat = actFrameMap.get(frameId).toJavaCVMat();

            int outputW = actMat.cols();
            int outputH = actMat.rows();
            opencv_core.Mat combineMat = new opencv_core.Mat();
            opencv_core.Size size = new opencv_core.Size(outputW, 2*outputH);
            opencv_imgproc.resize(actMat, combineMat, size);

            opencv_core.Mat dst_roi1 = new opencv_core.Mat(combineMat, new opencv_core.Rect(0, 0, outputW, outputH));
            trajMat.copyTo(dst_roi1);

            opencv_core.Mat dst_roi2 = new opencv_core.Mat(combineMat, new opencv_core.Rect(0, outputH, outputW, outputH));
            actMat.copyTo(dst_roi2);

            producer.addFrame(new StreamFrame(frameId, combineMat));
            trajFrameMap.remove(frameId);
            actFrameMap.remove(frameId);
        }

        if (toDebug) {
            System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        }
        collector.ack(tuple);
    }
}

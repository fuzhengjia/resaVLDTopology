package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tool.RedisStreamProducerBeta;
import topology.Serializable;
import topology.StreamFrame;
import util.ConfigUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * <p>
 * Strange issue, need to use RedisStreamProducerBeta????
 */
public class frameDisplayMulti extends BaseRichBolt {
    OutputCollector collector;

    //RedisStreamProducer producer;
    RedisStreamProducerBeta producer;

    private HashMap<Integer, Serializable.Mat> rawFrameMap;
    private HashMap<Integer, List<List<PointDesc>>> traceData;

//    private String host;
//    private int port;
//    private String queueName;
//
//    private int sleepTime;
//    private int startFrameID;
//    private int maxWaitCount;
//    private int accumulateFrameSize;

    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static float[] fscales;
    static int ixyScale = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rawFrameMap = new HashMap<>();
        traceData = new HashMap<>();

//        host = getString(map, "redis.host");
//        port = getInt(map, "redis.port");
//        queueName = getString(map, "redis.queueName");
//
//        this.sleepTime = ConfigUtil.getInt(map, "sleepTime", 10);
//        this.startFrameID = ConfigUtil.getInt(map, "startFrameID", 1);
//        this.maxWaitCount = ConfigUtil.getInt(map, "maxWaitCount", 4);
//        accumulateFrameSize = ConfigUtil.getInt(map, "accumulateFrameSize", 1);

        fscales = new float[scale_num];
        for (int i = 0; i < scale_num; i++) {
            fscales[i] = (float) Math.pow(scale_stride, i);
        }

//        producer = new RedisStreamProducerBeta(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
//        //producer = new RedisStreamProducer(host, port, queueName, accumulateFrameSize);
//        new Thread(producer).start();
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        if (frameId == 0) {
            collector.ack(tuple);
            return;
        }
        //System.out.println("receive tuple, frameID: " + frameId + ", streamID: " + streamId);
        IplImage fake = new IplImage();
        if (streamId.equals(STREAM_FRAME_OUTPUT)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            rawFrameMap.computeIfAbsent(frameId, k -> sMat);

        } else if (streamId.equals(STREAM_PLOT_TRACE)) {
            List<List<PointDesc>> traceRecords = (List<List<PointDesc>>) tuple.getValueByField(FIELD_TRACE_RECORD);
            traceData.computeIfAbsent(frameId, k -> traceRecords);
        }

        if (rawFrameMap.containsKey(frameId) && traceData.containsKey(frameId)) {
            Mat orgMat = rawFrameMap.get(frameId).toJavaCVMat();
            IplImage frame = orgMat.asIplImage();
            List<List<PointDesc>> traceRecords = traceData.get(frameId);
            for (List<PointDesc> trace : traceRecords) {
                float length = trace.size();
                float point0_x = fscales[ixyScale] * trace.get(0).sPoint.x();
                float point0_y = fscales[ixyScale] * trace.get(0).sPoint.y();
                CvPoint2D32f point0 = new CvPoint2D32f();
                point0.x(point0_x);
                point0.y(point0_y);

                float jIndex = 0;
                for (int jj = 1; jj < length; jj++, jIndex++) {
                    float point1_x = fscales[ixyScale] * trace.get(jj).sPoint.x();
                    float point1_y = fscales[ixyScale] * trace.get(jj).sPoint.y();
                    CvPoint2D32f point1 = new CvPoint2D32f();
                    point1.x(point1_x);
                    point1.y(point1_y);

                    cvLine(frame, cvPointFrom32f(point0), cvPointFrom32f(point1),
                            CV_RGB(cvFloor(255.0 * (jIndex + 1.0) / length), 0, 0), 1, 8, 0);
                    point0 = point1;
                }
            }

            Mat mat = new Mat(frame);
            Serializable.Mat sMat = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMat));
            //producer.addFrame(new StreamFrame(frameId, mat));
            System.out.println("FrameDisplay-finishedAdd: " + frameId + ", tCnt: " + traceRecords.size()
                    + "@" + System.currentTimeMillis());
            rawFrameMap.remove(frameId);
            traceData.remove(frameId);
        } else {
            System.out.println("finished: " + System.currentTimeMillis() + ":" + frameId
                    + ",rawFrameMap(" + rawFrameMap.containsKey(frameId) + ").Size: " + rawFrameMap.size()
                    + ",traceData(" + traceData.containsKey(frameId) + ").Size: " + traceData.size());
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

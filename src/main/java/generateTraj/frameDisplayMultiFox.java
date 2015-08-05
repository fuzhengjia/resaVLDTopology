package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tool.Serializable;
import util.ConfigUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * <p>
 * Strange issue, need to use RedisStreamProducerBeta????
 * In this version, we change some data formats
 */
public class frameDisplayMultiFox extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, Serializable.Mat> rawFrameMap;
    private HashMap<Integer, List<List<Serializable.CvPoint2D32f>>> traceData;
    private HashMap<Integer, Integer> traceMonitor;


    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static float[] fscales;
    static int ixyScale = 0;

    String traceAggBoltNameString;
    int traceAggBoltTaskNumber;
    private boolean toDebug = false;

    public frameDisplayMultiFox(String traceAggBoltNameString) {
        this.traceAggBoltNameString = traceAggBoltNameString;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rawFrameMap = new HashMap<>();
        traceData = new HashMap<>();
        traceMonitor = new HashMap<>();

        this.traceAggBoltTaskNumber = topologyContext.getComponentTasks(traceAggBoltNameString).size();
        toDebug = ConfigUtil.getBoolean(map, "debugTopology", false);

        fscales = new float[scale_num];
        for (int i = 0; i < scale_num; i++) {
            fscales[i] = (float) Math.pow(scale_stride, i);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        IplImage fake = new IplImage();

        if (frameId == 0) {
            collector.ack(tuple);
            return;
        }
        //System.out.println("receive tuple, frameID: " + frameId + ", streamID: " + streamId);
        if (streamId.equals(STREAM_FRAME_OUTPUT)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            rawFrameMap.computeIfAbsent(frameId, k -> sMat);

        } else if (streamId.equals(STREAM_PLOT_TRACE)) {
            List<List<Serializable.CvPoint2D32f>> traceRecords = (List<List<Serializable.CvPoint2D32f>>) tuple.getValueByField(FIELD_TRACE_RECORD);
            if (!traceMonitor.containsKey(frameId)){
                traceMonitor.put(frameId, 1);
                traceData.put(frameId, traceRecords);
            }else {
                traceMonitor.computeIfPresent(frameId, (k,v)->v+1);
                traceData.get(frameId).addAll(traceRecords);
            }
        }

        if (rawFrameMap.containsKey(frameId) && traceData.containsKey(frameId)
                && traceMonitor.get(frameId) == this.traceAggBoltTaskNumber) {

            collector.emit(STREAM_CACHE_CLEAN, new Values(frameId));

            Mat orgMat = rawFrameMap.get(frameId).toJavaCVMat();
            IplImage frame = orgMat.asIplImage();
            List<List<Serializable.CvPoint2D32f>> traceRecords = traceData.get(frameId);
            for (List<Serializable.CvPoint2D32f> trace : traceRecords) {
                float length = trace.size();
                float point0_x = fscales[ixyScale] * trace.get(0).x();
                float point0_y = fscales[ixyScale] * trace.get(0).y();
                CvPoint2D32f point0 = new CvPoint2D32f();
                point0.x(point0_x);
                point0.y(point0_y);

                float jIndex = 0;
                for (int jj = 1; jj < length; jj++, jIndex++) {
                    float point1_x = fscales[ixyScale] * trace.get(jj).x();
                    float point1_y = fscales[ixyScale] * trace.get(jj).y();
                    CvPoint2D32f point1 = new CvPoint2D32f();
                    point1.x(point1_x);
                    point1.y(point1_y);

                    cvLine(frame, cvPointFrom32f(point0), cvPointFrom32f(point1),
                            CV_RGB(0, cvFloor(255.0 * (jIndex + 1.0) / length), 0), 1, 8, 0);
                    point0 = point1;
                }
            }

            Mat mat = new Mat(frame);
            Serializable.Mat sMat = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMat));
            rawFrameMap.remove(frameId);
            traceData.remove(frameId);
            if (toDebug) {
                System.out.println("FrameDisplay-finishedAdd: " + frameId + ", tCnt: " + traceRecords.size()
                        + "@" + System.currentTimeMillis());
            }
        } else {
            if (toDebug) {
                System.out.println("finished: " + System.currentTimeMillis() + ":" + frameId
                        + ",rawFrameMap(" + rawFrameMap.containsKey(frameId) + ").Size: " + rawFrameMap.size()
                        + ",traceData(" + traceData.containsKey(frameId) + ").Size: " + traceData.size());
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        outputFieldsDeclarer.declareStream(STREAM_CACHE_CLEAN, new Fields(FIELD_FRAME_ID));
    }
}

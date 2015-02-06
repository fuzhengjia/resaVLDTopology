package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import util.ConfigUtil;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constant.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class traceGenerator extends BaseRichBolt {
    OutputCollector collector;

    private Map<Integer, boolean[]> indicatorList;
    private static int MaxSizeOfReceivedUpdatesFrom = 1048;

    IplImage grey, eig;
    IplImagePyramid eig_pyramid;

    double min_distance;
    double quality;
    int init_counter;

    long tracerIDCnt;
    int thisTaskID;

    DescInfo mbhInfo;

    static int patch_size = 32;
    static int nxy_cell = 2;
    static int nt_cell = 3;
    static float min_flow = 0.4f * 0.4f;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        thisTaskID = topologyContext.getThisTaskId();
        tracerIDCnt = 0;

        this.grey = null;
        this.eig = null;
        this.eig_pyramid = null;

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        this.mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);

        indicatorList = new LinkedHashMap<Integer, boolean[]>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, boolean[]> eldest) {
                return size() > MaxSizeOfReceivedUpdatesFrom;
            }
        };
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        System.out.println("receive tuple, frameID: " + frameId + ", streamID: " + streamId);

        if (streamId.equals(STREAM_NEW_TRACE)) {///from traceInit bolt
            try {
                LastPoint lastPoint = (LastPoint) tuple.getValueByField(FIELD_TRACE_LAST_POINT);

                int countersIndex = tuple.getIntegerByField(FIELD_COUNTERS_INDEX);

                int width = lastPoint.getW();
                int height = lastPoint.getH();

                int x = lastPoint.getX();
                int y = lastPoint.getY();

                int correspondingFrameID = frameId - init_counter;
                ///if init_counter = 1, the correspondingFrameID is the previous frame
                if (indicatorList.containsKey(correspondingFrameID)) {
                    if (indicatorList.get(correspondingFrameID)[countersIndex] == true) {
                        System.out.println("frame from Stream new trace, id: " + frameId + ", cIndex: " + countersIndex);
                        collector.ack(tuple);
                        return;
                    }
                }
                String traceID = generateTraceID(frameId);
                PointDesc point = new PointDesc(this.mbhInfo, cvPoint2D32f(x, y));
                TraceRecord trace = new TraceRecord(traceID, width, height, point);
                collector.emit(STREAM_EXIST_TRACE, new Values(frameId, trace));
                collector.emit(STREAM_REGISTER_TRACE, new Values(frameId, trace.traceID));///to the last bolt
                System.out.println("Generate new trace, id: " + frameId + "," + trace.traceID);
            }catch (Exception e){
                e.printStackTrace();
            }
        } else if (streamId.equals(STREAM_RENEW_TRACE)) {
            //from traceFilter bolt, field grouping by x, y of last point of each trace!!!
            TraceRecord trace = (TraceRecord) tuple.getValueByField(FIELD_TRACE_RECORD);
            int countersIndex = tuple.getIntegerByField(FIELD_COUNTERS_INDEX);
            ///here can be optimized to only use sPoint object instead of CvPoint2D32f object
            CvPoint2D32f point = new CvPoint2D32f(trace.pointDescs.getLast().sPoint.toJavaCvPoint2D32f());
            int width = trace.width;
            int height = trace.height;

            int x = cvFloor(point.x() / min_distance);
            int y = cvFloor(point.y() / min_distance);
            int ywx = y * width + x;

            if(ywx != countersIndex){
                throw new IllegalArgumentException("ywx: " + ywx + "!=countersIndex: " + countersIndex);}

            if (point.x() < min_distance * width && point.y() < min_distance * height) {
                ///create new entry if key traceID does not exist, otherwise, set the ywx position to TRUE;
                indicatorList.computeIfAbsent(frameId, (k) -> (new boolean[width * height]))[ywx] = true;
            }

            ///Caustion, we first update this trace to make the matching traceID ++, then remove the anchor!
            ///For shuffle grouping??
            int reNewedFrameID = frameId + 1;
            String traceID = generateTraceID(reNewedFrameID);
            trace.traceID = traceID;
            collector.emit(STREAM_EXIST_TRACE, new Values(reNewedFrameID, trace));
            collector.emit(STREAM_REGISTER_TRACE, new Values(reNewedFrameID, trace.traceID));///to the last bolt
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_EXIST_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        outputFieldsDeclarer.declareStream(STREAM_REGISTER_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_IDENTIFIER));
    }

    public String generateTraceID(int frameID) {
        return "t-" + thisTaskID + "-" + "f-" + frameID + "-" + (this.tracerIDCnt++);
    }

}

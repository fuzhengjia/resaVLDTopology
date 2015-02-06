package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import topology.Serializable;
import util.ConfigUtil;

import java.nio.FloatBuffer;
import java.util.*;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constant.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class optFlowTracker extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, opencv_core.IplImage> optFlowMap;
    private HashMap<Integer, Queue<TraceRecord>> traceQueue;
    int maxTrackerLength;
    DescInfo mbhInfo;
    double min_distance;

    static int patch_size = 32;
    static int nxy_cell = 2;
    static int nt_cell = 3;
    static float min_flow = 0.4f * 0.4f;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);
        this.mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
        optFlowMap = new HashMap<>();
        traceQueue = new HashMap<>();
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        if (streamId.equals(STREAM_EXIST_TRACE)) {
            TraceRecord trace = (TraceRecord) tuple.getValueByField(FIELD_TRACE_RECORD);

            ///if KEY frameID does not exist, then create one, then add to the entry
            this.traceQueue.computeIfAbsent(frameId, k-> new LinkedList<>()).add(trace);

            ///If find both, process
            if (optFlowMap.containsKey(frameId)) {
                opencv_core.IplImage flow = optFlowMap.get(frameId);
                processTraceRecords(flow, frameId);
            }

        } else if (streamId.equals(STREAM_OPT_FLOW)) {
            opencv_core.IplImage fake = new opencv_core.IplImage();
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            opencv_core.IplImage flow = sMat.toJavaCVMat().asIplImage();

            optFlowMap.computeIfAbsent(frameId, k->flow);

            if (traceQueue.containsKey(frameId)) {
                processTraceRecords(flow, frameId);
            }

        } else if (streamId.equals(STREAM_CACHE_CLEAN)) {
            optFlowMap.remove(frameId);
            traceQueue.remove(frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_EXIST_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        outputFieldsDeclarer.declareStream(STREAM_RENEW_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD, FIELD_TRACE_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_REMOVE_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_IDENTIFIER));
    }

    public void processTraceRecords(IplImage flow, int frameId) {
        Queue<TraceRecord> traceRecords = traceQueue.get(frameId);
        while (!traceRecords.isEmpty()) {
            TraceRecord trace = traceRecords.poll();
            CvPoint2D32f pointOut = getNextFlowPoint(flow, trace);
            if (pointOut != null) {
                PointDesc point = new PointDesc(mbhInfo, pointOut);
                trace.pointDescs.addLast(point);

                if (trace.pointDescs.size() > maxTrackerLength) {
                    ///Plot, but not feedback
                    collector.emit(STREAM_EXIST_TRACE, new Values(frameId, trace));
                } else {
                    ///Plot and feedback
                    int x = cvFloor(pointOut.x() / min_distance);
                    int y = cvFloor(pointOut.y() / min_distance);

                    LastPoint lp = new LastPoint(x, y, trace.width, trace.height);
                    collector.emit(STREAM_RENEW_TRACE, new Values(frameId, trace, lp.getFieldString()));
                }
            } else {
                ///Drop
                collector.emit(STREAM_REMOVE_TRACE, new Values(frameId, trace.traceID));
            }
        }
    }

    public CvPoint2D32f getNextFlowPoint(IplImage flow, TraceRecord trace) {

        int width = flow.width();
        int height = flow.height();

        CvPoint2D32f point_in = trace.pointDescs.getLast().sPoint.toJavaCvPoint2D32f();
        int x = cvFloor(point_in.x());
        int y = cvFloor(point_in.y());

        LinkedList<Float> xs = new LinkedList<>();
        LinkedList<Float> ys = new LinkedList<>();
        for (int m = x - 1; m <= x + 1; m++) {
            for (int n = y - 1; n <= y + 1; n++) {
                int p = Math.min(Math.max(m, 0), width - 1);
                int q = Math.min(Math.max(n, 0), height - 1);

                FloatBuffer floatBuffer = flow.getByteBuffer(q * flow.widthStep()).asFloatBuffer();
                int xsIndex = 2 * p;
                int ysIndex = 2 * p + 1;

                xs.addLast(floatBuffer.get(xsIndex));
                ys.addLast(floatBuffer.get(ysIndex));
            }
        }
        xs.sort(Float::compare);
        ys.sort(Float::compare);

        //TODO: need to optimize
        int size = xs.size() / 2;
        for (int m = 0; m < size; m++) {
            xs.removeLast();
            ys.removeLast();
        }

        CvPoint2D32f offset = new CvPoint2D32f();
        offset.x(xs.getLast());
        offset.y(ys.getLast());

        CvPoint2D32f point_out = new CvPoint2D32f();
        point_out.x(point_in.x() + offset.x());
        point_out.y(point_in.y() + offset.y());

        if (point_out.x() > 0 && point_out.x() < width && point_out.y() > 0 && point_out.y() < height) {
            return point_out;
        } else {
            return null;
        }
    }
}

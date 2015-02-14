package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import topology.Serializable;
import util.ConfigUtil;

import java.util.*;

import static org.bytedeco.javacpp.opencv_core.IplImage;
import static org.bytedeco.javacpp.opencv_core.cvFloor;
import static tool.Constant.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 *
 * In this gamma version, we re-design this traceAgg, in particular, do we really need to tracking all the traces by traceID?
 * what if we just monitor on the count?
 */
public class traceAggregatorGamma extends BaseRichBolt {
    OutputCollector collector;

    //private HashMap<Integer, HashSet<String>> traceMonitor;
    private HashMap<Integer, Integer> traceMonitor;
    private HashMap<String, List<PointDesc>> traceData;
    private HashMap<Integer, Queue<Object>> messageQueue;
    private HashMap<Integer, TwoIntegers> newPointsWHInfo;
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
        traceMonitor = new HashMap<>();
        traceData = new HashMap<>();
        messageQueue = new HashMap<>();
        newPointsWHInfo = new HashMap<>();

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);
        this.mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);

        IplImage fk = new IplImage();
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        if (streamId.equals(STREAM_EXIST_TRACE) || streamId.equals(STREAM_REMOVE_TRACE)) {
            Object message = tuple.getValueByField(FIELD_TRACE_IDENTIFIER);
            messageQueue.computeIfAbsent(frameId, k -> new LinkedList<>()).add(message);

        } else if (streamId.equals(STREAM_REGISTER_TRACE)) {
            //List<String> registerTraceIDList = (List<String>) tuple.getValueByField(FIELD_TRACE_IDENTIFIER);
            int registerTraceCnt = tuple.getIntegerByField(FIELD_TRACE_IDENTIFIER);
            TwoIntegers wh = (TwoIntegers)tuple .getValueByField(FIELD_WIDTH_HEIGHT);
            newPointsWHInfo.put(frameId, wh);

            ///TODO: to deal with special case when registerTraceIDList is empty!!!
            ///TODO: one point to optimize, the register for feedback traces are not necessary, can directly added in this bolt.
            //HashSet<String> traceIDset = new HashSet<>();
            //registerTraceIDList.forEach(k -> traceIDset.add(k));
            //traceMonitor.put(frameId, traceIDset);
            if (frameId == 1){
                traceMonitor.put(frameId, 0);
            }
            if (!traceMonitor.containsKey(frameId)){
                throw new IllegalArgumentException("!traceMonitor.containsKey(frameId), frameID: " + frameId);
            }
            int oldTraceCnt = traceMonitor.get(frameId);
            int newTraceCnt = traceMonitor.computeIfPresent(frameId, (k, v)-> v+registerTraceCnt);
            System.out.println("Register frame: " + frameId
                    + ", registerTraceCnt: " + registerTraceCnt
                    + ", traceMonitorCnt: " + traceMonitor.size()
                    + ", messageQueueSize: " + messageQueue.size()
                    + ", newPointsWHInfoSize: " + newPointsWHInfo.size()
                    + ", registeredOld: " + oldTraceCnt
                    + ", totalRegistered: " + newTraceCnt + "=" + traceMonitor.get(frameId));
        }

        if (traceMonitor.containsKey(frameId) && messageQueue.containsKey(frameId) && newPointsWHInfo.containsKey(frameId)) {
            aggregateTraceRecords(frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        outputFieldsDeclarer.declareStream(STREAM_CACHE_CLEAN, new Fields(FIELD_FRAME_ID));
        outputFieldsDeclarer.declareStream(STREAM_RENEW_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_INDICATOR_TRACE, new Fields(FIELD_FRAME_ID, FIELD_COUNTERS_INDEX));
    }

    public void aggregateTraceRecords(int frameId) {
        Queue<Object> messages = messageQueue.get(frameId);
        int messageSize = messages.size();
        //HashSet<String> traceIDset = traceMonitor.get(frameId);
        TwoIntegers wh = newPointsWHInfo.get(frameId);

        traceMonitor.computeIfPresent(frameId, (k,v)->v-messageSize);
        while (!messages.isEmpty()) {
            Object m = messages.poll();
            if (m instanceof TraceMetaAndLastPoint) {
                ///m  is from Exist_trace
                TraceMetaAndLastPoint trace = (TraceMetaAndLastPoint) m;
                traceData.computeIfAbsent(trace.traceID, k -> new ArrayList<>()).add(new PointDesc(mbhInfo, trace.lastPoint));

            } else if (m instanceof String) {
                String traceID2Remove = (String) m;
                traceData.computeIfPresent(traceID2Remove, (k, v) -> traceData.remove(k));
            }
        }

        if (traceMonitor.get(frameId) == 0) {//all traces are processed.
            List<List<PointDesc>> traceRecords = new ArrayList<List<PointDesc>>(traceData.values());
            //List<List<PointDesc>> traceRecords = traceData.values().stream().collect(Collectors.toList());
            collector.emit(STREAM_PLOT_TRACE, new Values(frameId, traceRecords));
            collector.emit(STREAM_CACHE_CLEAN, new Values(frameId));

            //List<TraceMetaAndLastPoint> feedbackPoints = new ArrayList<>();
            List<Integer> feedbackIndicators = new ArrayList<>();
            //HashSet<String> traceToRegister = new HashSet<>();
            int traceToRegisterCnt = 0;
            List<String> traceToRemove = new ArrayList<>();
            int width = wh.getV1();
            int height = wh.getV2();
            int nextFrameID = frameId + 1;
            //System.out.println("beforeRemove, traceDataSize: " + traceData.size());
            for (Map.Entry<String, List<PointDesc>> trace: traceData.entrySet()) {
                //traceData.forEach((k, v) -> {
                int traceLen = trace.getValue().size();
                if (traceLen > maxTrackerLength) {
                    traceToRemove.add(trace.getKey());
                } else {
                    //traceToRegister.add(trace.getKey());
                    traceToRegisterCnt++;
                    Serializable.CvPoint2D32f point = new Serializable.CvPoint2D32f(trace.getValue().get(traceLen - 1).sPoint);
                    TraceMetaAndLastPoint fdPt = new TraceMetaAndLastPoint(trace.getKey(), point);

                    int x = cvFloor(point.x() / min_distance);
                    int y = cvFloor(point.y() / min_distance);
                    int ywx = y * width + x;

                    if (point.x() < min_distance * width && point.y() < min_distance * height) {
                        feedbackIndicators.add(ywx);
                    }

                    collector.emit(STREAM_RENEW_TRACE, new Values(nextFrameID, fdPt));
                }
            }

            collector.emit(STREAM_INDICATOR_TRACE, new Values(nextFrameID, feedbackIndicators));
            traceToRemove.forEach(item -> traceData.remove(item));
            traceMonitor.remove(frameId);
            messageQueue.remove(frameId);
            newPointsWHInfo.remove(frameId);
            traceMonitor.put(nextFrameID, traceToRegisterCnt);

            System.out.println("ef: " + frameId + ", tMCnt: " + traceMonitor.size()
                    + ", mQS: " + messageQueue.size() + ", nPWHS: " + newPointsWHInfo.size()
                    + "tDS: " + traceData.size() + ", removeSize: " + traceToRemove.size() + ", exisSize: " + traceToRegisterCnt);
        }
    }
}

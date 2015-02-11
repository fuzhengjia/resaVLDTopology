package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.NullArgumentException;
import topology.Serializable;
import util.ConfigUtil;

import java.lang.reflect.Array;
import java.nio.FloatBuffer;
import java.util.*;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constant.*;
import static topology.Constants.CACHE_CLEAR_STREAM;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class traceAggregator extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, HashSet<String>> traceMonitor;
    private HashMap<String, List<PointDesc>> traceData;
    private HashMap<Integer, Queue<Object>> messageQueue;
    int maxTrackerLength;
    DescInfo mbhInfo;

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

        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);
        this.mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
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
            List<String> registerTraceIDList = (List<String>) tuple.getValueByField(FIELD_TRACE_IDENTIFIER);
            ///TODO: to deal with special case when registerTraceIDList is empty!!!
            HashSet<String> traceIDset = new HashSet<>();
            registerTraceIDList.forEach(k -> traceIDset.add(k));
            System.out.println("Register frame: " + frameId
                            + ", registerTraceListCnt: " + registerTraceIDList.size()
                            + ", traceSetSize: " + traceIDset.size()
                            + ", traceMonitorCnt: " + traceMonitor.size()
                            + ", messageQueueSize: " + messageQueue.size()
            );
            traceMonitor.put(frameId, traceIDset);
        }

        if (traceMonitor.containsKey(frameId) && messageQueue.containsKey(frameId)) {
            aggregateTraceRecords(frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        outputFieldsDeclarer.declareStream(STREAM_CACHE_CLEAN, new Fields(FIELD_FRAME_ID));
        outputFieldsDeclarer.declareStream(STREAM_RENEW_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
    }

    public void aggregateTraceRecords(int frameId) {
        Queue<Object> messages = messageQueue.get(frameId);
        HashSet<String> traceIDset = traceMonitor.get(frameId);
        while (!messages.isEmpty()) {
            Object m = messages.poll();
            if (m instanceof TraceMetaAndLastPoint) {
                ///m  is from Exist_trace
                TraceMetaAndLastPoint trace = (TraceMetaAndLastPoint) m;
                if (!traceIDset.contains(trace.traceID)) {
                    throw new IllegalArgumentException("traceIDset.contains(trace.traceID) is false, frameID: " + frameId + ",tID: " + trace.traceID);
                } else {
                    traceIDset.remove(trace.traceID);
                }
                traceData.computeIfAbsent(trace.traceID, k -> new ArrayList<>()).add(new PointDesc(mbhInfo, trace.lastPoint));

            } else if (m instanceof String) {
                String traceID2Remove = (String) m;
                if (!traceIDset.contains(traceID2Remove)) {
                    throw new IllegalArgumentException("traceIDset.contains(trace.traceID) is false, frameID: " + frameId + ",tID: " + traceID2Remove);
                } else {
                    traceIDset.remove(traceID2Remove);
                }

                traceData.computeIfPresent(traceID2Remove, (k, v) -> traceData.remove(k));
            }
        }

        if (traceIDset.isEmpty()) {//all traces are processed.
            List<List<PointDesc>> traceRecords = new ArrayList<List<PointDesc>>(traceData.values());
            //List<List<PointDesc>> traceRecords = traceData.values().stream().collect(Collectors.toList());
            collector.emit(STREAM_PLOT_TRACE, new Values(frameId, traceRecords));
            collector.emit(STREAM_CACHE_CLEAN, new Values(frameId));
            traceMonitor.remove(frameId);
            messageQueue.remove(frameId);
            System.out.println("emit frame: " + frameId + ", traceMonitorCnt: "
                    + traceMonitor.size() + ", messageQueueSize: " + messageQueue.size());

            List<TraceMetaAndLastPoint> feedbackPoints = new ArrayList<>();
            List<String> traceToRemove = new ArrayList<>();
            //System.out.println("beforeRemove, traceDataSize: " + traceData.size());
            traceData.forEach((k, v) -> {
                if (v.size() > maxTrackerLength) {
                    traceToRemove.add(k);
                } else {
                    feedbackPoints.add(new TraceMetaAndLastPoint(k, v.get(v.size() - 1).sPoint));
                }
            });
            int nextFrameID = frameId + 1;
            collector.emit(STREAM_RENEW_TRACE, new Values(nextFrameID, feedbackPoints));
            traceToRemove.forEach(item -> traceData.remove(item));
            //System.out.println("AfterRemove, traceDataSize: " + traceData.size() + ", removedSize: " + traceToRemove.size());
        }
    }
}

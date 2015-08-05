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

import java.util.*;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * <p>
 * We design the delta version, to make traceAgg distributable, i.e., each instance takes a subset of all the traces,
 * partitioned by traceID
 */
public class traceAggFoxActDet extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, Integer> traceMonitor;
    private HashMap<String, List<Serializable.CvPoint2D32f>> traceData;
    private HashMap<Integer, Queue<Object>> messageQueue;
    private HashMap<Integer, List<TwoIntegers>> newPointsWHInfo;

    int maxTrackerLength;
    double min_distance;
    int init_counter;

    String traceGenBoltNameString;
    int traceGenBoltTaskNumber;
    List<Integer> flowTrackerTasks;
    String flowTrackerName;

    public traceAggFoxActDet(String traceGenBoltNameString, String flowTrackerName) {
        this.traceGenBoltNameString = traceGenBoltNameString;
        this.flowTrackerName = flowTrackerName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        traceMonitor = new HashMap<>();
        traceData = new HashMap<>();
        messageQueue = new HashMap<>();
        newPointsWHInfo = new HashMap<>();

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        this.traceGenBoltTaskNumber = topologyContext.getComponentTasks(traceGenBoltNameString).size();
        flowTrackerTasks = topologyContext.getComponentTasks(flowTrackerName);

    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        IplImage fk = new IplImage();

        if (streamId.equals(STREAM_EXIST_REMOVE_TRACE)) {
            List<Object> messages = (List<Object>) tuple.getValueByField(FIELD_TRACE_CONTENT);
            messageQueue.computeIfAbsent(frameId, k -> new LinkedList<>()).addAll(messages);

        } else if (streamId.equals(STREAM_REGISTER_TRACE)) {
            Integer registerTraceCnt = tuple.getIntegerByField(FIELD_TRACE_CONTENT);
            TwoIntegers wh = (TwoIntegers) tuple.getValueByField(FIELD_WIDTH_HEIGHT);

            System.out.println("DeepInAgg, registerCnt: " + registerTraceCnt + ", frameID: " + frameId);
            newPointsWHInfo.computeIfAbsent(frameId, k -> new ArrayList<>()).add(wh);
            ///TODO: to deal with special case when registerTraceIDList is empty!!!
            if (frameId == 1 && !traceMonitor.containsKey(frameId)) {
                traceMonitor.put(frameId, 0);
            }
            if (!traceMonitor.containsKey(frameId)) {
                throw new IllegalArgumentException("!traceMonitor.containsKey(frameId), frameID: " + frameId);
            }
            traceMonitor.computeIfPresent(frameId, (k, v) -> v + registerTraceCnt);
//            System.out.println("Register frame: " + frameId
//                    //+ ", registerTraceListCnt: " + registerTraceIDList.size()
//                    + ", registerTraceCnt: " + registerTraceCnt
//                    //+ ", traceSetSize: " + traceIDset.size()
//                    + ", traceMonitorCnt: " + traceMonitor.size()
//                    + ", messageQueueSize: " + messageQueue.size()
//                    + ", newPointsWHInfoSize: " + newPointsWHInfo.size()
//                    //+ ", totalRegistered: " + traceMonitor.get(frameId).size()
//                    );
        }

        if (traceMonitor.containsKey(frameId) && messageQueue.containsKey(frameId)
                && newPointsWHInfo.containsKey(frameId)
                && newPointsWHInfo.get(frameId).size() == traceGenBoltTaskNumber) {
            aggregateTraceRecords(frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FEATURE_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        outputFieldsDeclarer.declareStream(STREAM_RENEW_TRACE, true, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_INDICATOR_TRACE, new Fields(FIELD_FRAME_ID, FIELD_COUNTERS_INDEX));
    }

    public void aggregateTraceRecords(int frameId) {
        Queue<Object> messages = messageQueue.get(frameId);
        TwoIntegers wh = newPointsWHInfo.get(frameId).get(0);
        traceMonitor.computeIfPresent(frameId, (k, v) -> v - messages.size());

        while (!messages.isEmpty()) {
            Object m = messages.poll();
            if (m instanceof TraceMetaAndLastPoint) {
                ///m  is from Exist_trace
                TraceMetaAndLastPoint trace = (TraceMetaAndLastPoint) m;
                if (!traceData.containsKey(trace.traceID)) {
                    throw new IllegalArgumentException("!traceData.containsKey(trace.traceID), frameId: " + frameId + ", traceID: " + trace.traceID);
                }
                traceData.get(trace.traceID).add(new Serializable.CvPoint2D32f(trace.lastPoint));
            } else if (m instanceof String) {
                String traceID2Remove = (String) m;
                traceData.computeIfPresent(traceID2Remove, (k, v) -> traceData.remove(k));
            } else if (m instanceof NewTraceMeta) {
                NewTraceMeta trace = (NewTraceMeta) m;
                if (traceData.containsKey(trace.traceID)) {
                    throw new IllegalArgumentException("traceID already exist!, frameId: " + frameId + ", traceID: " + trace.traceID);
                }
                List<Serializable.CvPoint2D32f> newTrace = new ArrayList<>();
                newTrace.add(new Serializable.CvPoint2D32f(trace.firstPoint));
                newTrace.add(new Serializable.CvPoint2D32f(trace.nextPoint));
                traceData.put(trace.traceID, newTrace);
            }
        }

        if (traceMonitor.get(frameId) == 0) {
            ///List<List<Serializable.CvPoint2D32f>> traceRecords = new ArrayList<>(traceData.values());
            ///collector.emit(STREAM_PLOT_TRACE, new Values(frameId, traceRecords));

            List<Integer> feedbackIndicators = new ArrayList<>();
            int traceToRegisterCnt = 0;
            List<String> traceToRemove = new ArrayList<>();
            int frameWidth = wh.getV1();
            int frameHeight = wh.getV2();
            int eigWidth = cvFloor(frameWidth / min_distance);
            int eigHeight = cvFloor(frameHeight / min_distance);

            int nextFrameID = frameId + 1;
            List<List<TraceMetaAndLastPoint>> renewTraces = new ArrayList<>();
            for (int i = 0; i < flowTrackerTasks.size(); i++) {
                renewTraces.add(new ArrayList<>());
            }

//            System.out.println("DeepInAgg, traceCnt: " + traceData.size() + ", frameID: " + frameId);
//            for (Map.Entry<String, List<Serializable.CvPoint2D32f>> trace : traceData.entrySet()) {
//                String debInfo = "fID: " + frameId + ", tID: " + trace.getKey() + ", len: " + trace.getValue().size() + "-";
//                for (int kk = 0; kk < trace.getValue().size(); kk ++){
//                    debInfo += "(" + trace.getValue().get(kk).x() + "," + trace.getValue().get(kk).y() + ")->";
//                }
//                System.out.println(debInfo);
//            }
            int overLen = 0;
            int overLenValid = 0;
            List<List<Serializable.CvPoint2D32f>> traceForFeatures = new ArrayList<>();
            for (Map.Entry<String, List<Serializable.CvPoint2D32f>> trace : traceData.entrySet()) {
                int traceLen = trace.getValue().size();
                if (traceLen > maxTrackerLength) {
                    overLen ++;
                    if (helperFunctions.isValid(trace.getValue()) == 1) {
                        traceForFeatures.add(trace.getValue());
                        overLenValid++;
//                        String debInfo = null;
//                        debInfo = "fID: " + frameId + ", tID: " + trace.getKey() + ", len: " + trace.getValue().size() + "-";
//                        for (int kk = 0; kk < trace.getValue().size(); kk++) {
//                            debInfo += "(" + trace.getValue().get(kk).x() + "," + trace.getValue().get(kk).y() + ")->";
//                        }
//                        System.out.println(debInfo);
                    }
                    traceToRemove.add(trace.getKey());
                } else {
                    traceToRegisterCnt++;
                    Serializable.CvPoint2D32f point = new Serializable.CvPoint2D32f(trace.getValue().get(traceLen - 1));
                    TraceMetaAndLastPoint fdPt = new TraceMetaAndLastPoint(trace.getKey(), point);

                    int x = cvFloor(point.x() / min_distance);
                    int y = cvFloor(point.y() / min_distance);
                    int ywx = y * eigWidth + x;

                    if (point.x() < min_distance * eigWidth && point.y() < min_distance * eigHeight) {
                        feedbackIndicators.add(ywx);
                    }

                    ///Caution!!!
                    int q = Math.min(Math.max(cvFloor(point.y()), 0), frameHeight - 1);
                    renewTraces.get(q % flowTrackerTasks.size()).add(fdPt);
                }
            }
            System.out.println("DeepInAgg, traceCntAfter: " + traceData.size() + ", frameID: " + frameId + ",ol: " + overLen + ",olv: " + overLenValid + ", toRenew: " + traceToRegisterCnt);
            collector.emit(STREAM_FEATURE_TRACE, new Values(frameId, traceForFeatures));

            for (int i = 0; i < flowTrackerTasks.size(); i++) {
                int tID = flowTrackerTasks.get(i);
                collector.emitDirect(tID, STREAM_RENEW_TRACE, new Values(nextFrameID, renewTraces.get(i)));
            }

            traceToRemove.forEach(item -> traceData.remove(item));
            traceMonitor.remove(frameId);
            messageQueue.remove(frameId);
            newPointsWHInfo.remove(frameId);

            ///TODO: caution, here for feedback indicate, we should use the original frameID!
            if (frameId % init_counter == 0) {
                collector.emit(STREAM_INDICATOR_TRACE, new Values(frameId, feedbackIndicators));
            } else {
                List<TwoIntegers> empty = new ArrayList<>();
                for (int i = 0; i < traceGenBoltTaskNumber; i++) {
                    empty.add(new TwoIntegers(wh.getV1(), wh.getV2()));
                }
                newPointsWHInfo.put(nextFrameID, empty);
            }
            traceMonitor.put(nextFrameID, traceToRegisterCnt);

//            System.out.println("ef: " + frameId + ", tMCnt: " + traceMonitor.size()
//                    + ", mQS: " + messageQueue.size() + ", nPWHS: " + newPointsWHInfo.size()
//                    + "tDS: " + traceData.size() + ", removeSize: " + traceToRemove.size()
//                    + ", exisSize: " + traceToRegisterCnt
//            );
        }
    }
}

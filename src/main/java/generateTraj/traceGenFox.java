package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;
import util.ConfigUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.cvFloor;
import static org.bytedeco.javacpp.opencv_core.cvPoint2D32f;
import static tool.Constants.*;

/**
 * Created by Tom Fu, on July 30, 2015
 *
 * Fix a bug in the Echo version,
 * It should be the Prev_frame to generate the new Trace, instead of the later one!!
 *
 * 注意，为了在flowTracker那里区分新和旧的trace，需要在这里emit出List对象
 */
public class traceGenFox extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, List<Integer>> feedbackIndicatorList;
    private HashMap<Integer, Integer> feedbackMonitor;
    private HashMap<Integer, List<float[]>> eigFrameMap;
    private HashMap<Integer, EigRelatedInfo> eigInfoMap;

    double min_distance;
    double quality;
    int init_counter;

    private long tracerIDCnt;
    private int thisTaskID;

    private int thisTaskIndex;
    private int taskCnt;

    String traceAggBoltNameString;
    List<Integer> traceAggBoltTasks;
    List<Integer> flowTrackerTasks;
    String flowTrackerName;

    public traceGenFox(String traceAggBoltNameString, String flowTrackerName) {
        this.traceAggBoltNameString = traceAggBoltNameString;
        this.flowTrackerName = flowTrackerName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();

        this.thisTaskIndex = topologyContext.getThisTaskIndex();
        this.thisTaskID = topologyContext.getThisTaskId();

        this.tracerIDCnt = 0;
        this.traceAggBoltTasks = topologyContext.getComponentTasks(traceAggBoltNameString);
        this.flowTrackerTasks = topologyContext.getComponentTasks(flowTrackerName);

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        this.feedbackIndicatorList = new HashMap<>();
        eigFrameMap = new HashMap<>();
        eigInfoMap = new HashMap<>();
        this.feedbackMonitor = new HashMap<>();

        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        if (frameId % init_counter > 0) {
            throw new IllegalArgumentException("FrameID: " + frameId + ", init_counter: " + init_counter);
        }

        if (streamId.equals(STREAM_EIG_FLOW)) {///from traceInit bolt
            List<float[]> floatArray = (List<float[]>) tuple.getValueByField(FIELD_FRAME_MAT);
            eigFrameMap.computeIfAbsent(frameId, k -> floatArray);
            EigRelatedInfo eigInfo = (EigRelatedInfo) tuple.getValueByField(FIELD_EIG_INFO);
            eigInfoMap.computeIfAbsent(frameId, k -> eigInfo);

            ///This is to deal with the first special frame, where there are no feedback traces.
            if (frameId == 0) {
                feedbackMonitor.computeIfAbsent(frameId, k -> this.traceAggBoltTasks.size());
                feedbackIndicatorList.computeIfAbsent(frameId, k -> new ArrayList<>());
            }

        } else if (streamId.equals(STREAM_INDICATOR_TRACE)) {
            List<Integer> feedbackIndicators = (List<Integer>) tuple.getValueByField(FIELD_COUNTERS_INDEX);
            if (!feedbackMonitor.containsKey(frameId)) {
                feedbackMonitor.put(frameId, 1);
                feedbackIndicatorList.put(frameId, feedbackIndicators);
            } else {
                feedbackMonitor.computeIfPresent(frameId, (k, v) -> v + 1);
                feedbackIndicatorList.get(frameId).addAll(feedbackIndicators);
            }
        }

        ///Now, the two FrameID are synchronized!!!
        if (eigFrameMap.containsKey(frameId) && feedbackIndicatorList.containsKey(frameId)
                && feedbackMonitor.get(frameId) == traceAggBoltTasks.size()) {
            List<Integer> feedbackIndicators = feedbackIndicatorList.get(frameId);
            List<float[]> floatArray = eigFrameMap.get(frameId);
            EigRelatedInfo eigInfo = eigInfoMap.get(frameId);

            int frameWidth = eigInfo.getW();
            int frameHeight = eigInfo.getH();
            int eigWidth = cvFloor(frameWidth / min_distance);
            int eigHeight = cvFloor(frameHeight / min_distance);

            boolean[] counters = new boolean[eigWidth * eigHeight];
            if (feedbackIndicators.size() > 0) {
                for (int index : feedbackIndicators) {
                    counters[index] = true;
                }
            } else {
                //System.out.println("No new feedback points generated for frame: " + frameId);
            }

            int totalValidedCount = 0;
            int nextFrameID = frameId + 1; //after generate the new traces, change to the nextFrameID.
            int[] totalValidCntList = new int[this.traceAggBoltTasks.size()];
            if (frameId >= 0) {
                double threshold = eigInfo.getTh();
                int offset = eigInfo.getOff();

                List<List<NewTraceMeta>> newTraces = new ArrayList<>();
                for (int i = 0; i < flowTrackerTasks.size(); i++) {
                    newTraces.add(new ArrayList<>());
                }

                //System.out.println("i: " + this.thisTaskIndex + ", tID: " + this.thisTaskID + ", size: " + floatArray.size()
                //        + ",w: "+ width + ", h: " + height + ",off: " + offset + ", min_dis:" + min_distance);
                for (int i = 0; i < eigHeight; i++) {
                    ///only for particular rows
                    if (i % taskCnt == thisTaskIndex) {
                        for (int j = 0; j < eigWidth; j++) {
                            int ywx = i * eigWidth + j;
                            if (counters[ywx] == false) {
                                //FloatBuffer floatBuffer = eig.getByteBuffer(y * eig.widthStep()).asFloatBuffer();
                                //float ve = floatBuffer.get(x);
                                int x = opencv_core.cvFloor(j * min_distance + offset);
                                int y = opencv_core.cvFloor(i * min_distance + offset);
                                int rowIndex = i / this.taskCnt;
                                float[] fData = floatArray.get(rowIndex);
                                float ve = fData[x];

                                if (ve > threshold) {
                                    String traceID = generateTraceID(frameId);
                                    Serializable.CvPoint2D32f firstPt = new Serializable.CvPoint2D32f(cvPoint2D32f(x, y));
                                    NewTraceMeta newTrace = new NewTraceMeta(traceID, firstPt);
                                    totalValidedCount++;
                                    int tIDindex = Math.abs(traceID.hashCode()) % totalValidCntList.length;
                                    //System.out.println("traceID: " + traceID + ",tIDindex: " + tIDindex + ", totalValidCntList.Len: "  +totalValidCntList.length);
                                    totalValidCntList[tIDindex]++;

                                    //Caution, here is a bug, must NOT use eigHeight, but use frameHeight!!!
                                    int q = Math.min(Math.max(opencv_core.cvFloor(firstPt.y()), 0), frameHeight - 1);
                                    newTraces.get(q % flowTrackerTasks.size()).add(newTrace);
                                }
                            }
                        }
                    }
                }

                for (int i = 0; i < flowTrackerTasks.size(); i++) {
                    int tID = flowTrackerTasks.get(i);
                    collector.emitDirect(tID, STREAM_NEW_TRACE, new Values(nextFrameID, newTraces.get(i)));
                }

            } else {
                //System.out.println("No new dense point generated for frame: " + frameId);
            }
            //System.out.println("Frame: " + frameId + " emitted: " //+ registerTraceIDList.size()
            //       + ",validCnt: " + totalValidedCount + ",fd: " + feedbackIndicators.size());
            //collector.emit(STREAM_REGISTER_TRACE, new Values(frameId, registerTraceIDList, new TwoIntegers(width, height)));
            //collector.emit(STREAM_REGISTER_TRACE, new Values(frameId, totalValidedCount, new TwoIntegers(width, height)));
            for (int i = 0; i < totalValidCntList.length; i++) {
                int tID = this.traceAggBoltTasks.get(i);
                ///Caution!!! here shall be eigWidth, eigHeight
                collector.emitDirect(tID, STREAM_REGISTER_TRACE, new Values(nextFrameID, totalValidCntList[i], new TwoIntegers(frameWidth, frameHeight)));
            }
            this.feedbackIndicatorList.remove(frameId);
            this.eigFrameMap.remove(frameId);
            this.eigInfoMap.remove(frameId);
            this.feedbackMonitor.remove(frameId);
        } else {
//            System.out.println("FrameID: " + frameId + ", streamID: " + streamId
//                    + ", greyFrameMapCnt: " + eigFrameMap.size() + ",fbPointsListCnt: " + feedbackIndicatorList.size());
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_NEW_TRACE, true, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_REGISTER_TRACE, true, new Fields(FIELD_FRAME_ID, FIELD_TRACE_CONTENT, FIELD_WIDTH_HEIGHT));
    }

    public String generateTraceID(int frameID) {
        return thisTaskID + "-" + frameID + "-" + (this.tracerIDCnt++);
    }

}
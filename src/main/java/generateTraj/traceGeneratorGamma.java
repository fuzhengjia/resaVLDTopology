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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.cvPoint2D32f;
import static tool.Constant.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class traceGeneratorGamma extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, List<Integer>> feedbackIndicatorList;
    private HashMap<Integer, Serializable.Mat> eigFrameMap;
    private HashMap<Integer, EigRelatedInfo> eigInfoMap;

    double min_distance;
    double quality;
    int init_counter;

    private long tracerIDCnt;
    private int thisTaskID;

    private int thisTaskIndex;
    private int taskCntOfThisComponent;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.thisTaskIndex = topologyContext.getThisTaskIndex();
        this.taskCntOfThisComponent = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        thisTaskID = topologyContext.getThisTaskId();
        tracerIDCnt = 0;

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        this.feedbackIndicatorList = new HashMap<>();
        eigFrameMap = new HashMap<>();
        eigInfoMap = new HashMap<>();

        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        ///TODO: Make sure, this frameID ++ is done by the traceAgg bolt!!!
        ///TODO: be careful about the processing of init_counter, this should also collaborate with Feedback
        //if (streamId.equals(STREAM_RENEW_TRACE)) {
        //    frameId++;///here we adjust the frameID of renewTrace
        //}
        System.out.println("receive tuple, frameID: " + frameId + ", streamID: " + streamId);

        if (streamId.equals(STREAM_EIG_FLOW)) {///from traceInit bolt
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            eigFrameMap.computeIfAbsent(frameId, k -> sMat);
            EigRelatedInfo eigInfo = (EigRelatedInfo) tuple.getValueByField(FIELD_EIG_INFO);
            eigInfoMap.computeIfAbsent(frameId, k -> eigInfo);
            ///This is to deal with the first special frame, where there are no feedback traces.
            if (frameId == 1) {
                feedbackIndicatorList.computeIfAbsent(frameId, k -> new ArrayList<>());
            }

        } else if (streamId.equals(STREAM_INDICATOR_TRACE)) {
            List<Integer> feedbackIndicators = (List<Integer>) tuple.getValueByField(FIELD_COUNTERS_INDEX);
            feedbackIndicatorList.computeIfAbsent(frameId, k -> feedbackIndicators);
        }

        ///Now, the two FrameID are synchronized!!!
        if (eigFrameMap.containsKey(frameId) && feedbackIndicatorList.containsKey(frameId)) {
            List<Integer> feedbackIndicators = feedbackIndicatorList.get(frameId);
            opencv_core.Mat orgMat = eigFrameMap.get(frameId).toJavaCVMat();
            EigRelatedInfo eigInfo = eigInfoMap.get(frameId);

            int width = eigInfo.getW();
            int height = eigInfo.getH();

            boolean[] counters = new boolean[width * height];
            if (feedbackIndicators.size() > 0) {
                for (int index : feedbackIndicators) {
                    counters[index] = true;
                }
            } else {
                System.out.println("No new feedback points generated for frame: " + frameId);
            }

            List<String> registerTraceIDList = new ArrayList<>();
            int totalValidedCount = 0;
            if (frameId > 0 && frameId % init_counter == 0) {

                opencv_core.IplImage eig = orgMat.asIplImage();
                double threshold = eigInfo.getTh();
                int offset = eigInfo.getOff();

                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        int ywx = i * width + j;
                        if (ywx % taskCntOfThisComponent == thisTaskIndex) {
                            if (counters[ywx] == false) {
                                int x = opencv_core.cvFloor(j * min_distance + offset);
                                int y = opencv_core.cvFloor(i * min_distance + offset);
                                FloatBuffer floatBuffer = eig.getByteBuffer(y * eig.widthStep()).asFloatBuffer();
                                float ve = floatBuffer.get(x);

                                if (ve > threshold) {
                                    String traceID = generateTraceID(frameId);
                                    Serializable.CvPoint2D32f lastPt = new Serializable.CvPoint2D32f(cvPoint2D32f(x, y));
                                    TraceMetaAndLastPoint newTrace = new TraceMetaAndLastPoint(traceID, lastPt);
                                    totalValidedCount++;
                                    registerTraceIDList.add(newTrace.traceID);
                                    collector.emit(STREAM_NEW_TRACE, new Values(frameId, newTrace));
                                }
                            }
                        }
                    }
                }
            } else {
                System.out.println("No new dense point generated for frame: " + frameId);
            }
            System.out.println("Frame: " + frameId + " emitted: " + registerTraceIDList.size()
                    + ",validCnt: " + totalValidedCount + ",fd: " + feedbackIndicators.size());
            collector.emit(STREAM_REGISTER_TRACE, new Values(frameId, registerTraceIDList, new TwoIntegers(width, height)));
            this.feedbackIndicatorList.remove(frameId);
            this.eigFrameMap.remove(frameId);
            this.eigInfoMap.remove(frameId);
        } else {
            System.out.println("FrameID: " + frameId + ", streamID: " + streamId
                    + ", greyFrameMapCnt: " + eigFrameMap.size() + ",fbPointsListCnt: " + feedbackIndicatorList.size());
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_NEW_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_REGISTER_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_CONTENT, FIELD_WIDTH_HEIGHT));
    }

    public String generateTraceID(int frameID) {
        return thisTaskID + "-" + frameID + "-" + (this.tracerIDCnt++);
    }

}
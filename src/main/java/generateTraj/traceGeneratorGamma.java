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

    private HashMap<Integer, List<NewDensePoint>> newPointsList;
    private HashMap<Integer, TwoIntegers> newPointsWHInfo;
    private HashMap<Integer, List<Integer>> feedbackIndicatorList;

    double min_distance;
    double quality;
    int init_counter;

    long tracerIDCnt;
    int thisTaskID;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        thisTaskID = topologyContext.getThisTaskId();
        tracerIDCnt = 0;

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        this.newPointsList = new HashMap<>();
        this.newPointsWHInfo = new HashMap<>();
        this.feedbackIndicatorList = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        ///TODO: Make sure, this frameID ++ is done by the traceAgg bolt!!!
        //if (streamId.equals(STREAM_RENEW_TRACE)) {
        //    frameId++;///here we adjust the frameID of renewTrace
        //}
        System.out.println("receive tuple, frameID: " + frameId + ", streamID: " + streamId);

        if (streamId.equals(STREAM_NEW_TRACE)) {///from traceInit bolt
            List<NewDensePoint> newPoints = (List<NewDensePoint>) tuple.getValueByField(FIELD_NEW_POINTS);
            TwoIntegers wh = (TwoIntegers) tuple.getValueByField(FIELD_WIDTH_HEIGHT);

            if (!newPointsList.containsKey(frameId)) {
                newPointsList.put(frameId, newPoints);
                newPointsWHInfo.put(frameId, wh);
            }
            ///This is to deal with the first special frame, where there are no feedback traces.
            if (frameId == 1) {
                List<Integer> emptySet = new ArrayList<>();
                feedbackIndicatorList.put(frameId, emptySet);
            }

        } else if (streamId.equals(STREAM_INDICATOR_TRACE)) {
            List<Integer> feedbackIndicators = (List<Integer>) tuple.getValueByField(FIELD_COUNTERS_INDEX);
            if (!feedbackIndicatorList.containsKey(frameId)) {
                feedbackIndicatorList.put(frameId, feedbackIndicators);
            }
        }

        ///Now, the two FrameID are synchronized!!!
        //if (newPointsList.containsKey(frameId) && feedbackPointsList.containsKey(frameId)) {
        if (newPointsList.containsKey(frameId) && feedbackIndicatorList.containsKey(frameId)) {
            List<NewDensePoint> newPoints = newPointsList.get(frameId);
            TwoIntegers wh = newPointsWHInfo.get(frameId);
            List<Integer> feedbackIndicators = feedbackIndicatorList.get(frameId);

            int width = wh.getV1();
            int height = wh.getV2();
            ///Make sure, the width and height information are valid!

            boolean[] counters = new boolean[width * height];
            if (feedbackIndicators.size() > 0) {
                for (int index : feedbackIndicators) {
                    counters[index] = true;
                }
            } else {
                System.out.println("No new feedback points generated for frame: " + frameId);
            }

            int totalValidedCount = 0;
            if (newPoints.size() > 0) {
                for (NewDensePoint newPt : newPoints) {
                    int x = newPt.getX();
                    int y = newPt.getY();
                    ///causion, here must use i and j to calculate
                    int ywx = newPt.getY_I() * width + newPt.getX_J();

                    if (counters[ywx] == false) {
                        String traceID = generateTraceID(frameId);
                        Serializable.CvPoint2D32f lastPt = new Serializable.CvPoint2D32f(cvPoint2D32f(x, y));
                        TraceMetaAndLastPoint newTrace = new TraceMetaAndLastPoint(traceID, lastPt);
                        totalValidedCount++;
                        collector.emit(STREAM_NEW_TRACE, new Values(frameId, newTrace));
                    }
                }
            } else {
                System.out.println("No new dense point generated for frame: " + frameId);
            }
            System.out.println("Frame: " + frameId + " valid and emitted: " + totalValidedCount
                    + ", newPt: " + newPoints.size() + ",fd: " + feedbackIndicators.size());
            collector.emit(STREAM_REGISTER_TRACE, new Values(frameId, totalValidedCount, wh));
            this.newPointsList.remove(frameId);
            this.newPointsWHInfo.remove(frameId);
            this.feedbackIndicatorList.remove(frameId);
        } else {
            System.out.println("FrameID: " + frameId + ", streamID: " + streamId
                    + ", newListCnt: " + newPointsList.size() + ",fbPointsListCnt: " + feedbackIndicatorList.size());
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_NEW_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_REGISTER_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_IDENTIFIER, FIELD_WIDTH_HEIGHT));
    }

    public String generateTraceID(int frameID) {
        return thisTaskID + "-" + frameID + "-" + (this.tracerIDCnt++);
    }

}
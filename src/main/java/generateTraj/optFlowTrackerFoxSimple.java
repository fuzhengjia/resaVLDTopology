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

import java.nio.FloatBuffer;
import java.util.*;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu, July 30, 2015
 *
 * 从optFlowTransEcho传来的是List<float【】>
 */
///TODO: maybe move the mbhInfo generation to another bolt, which also needs opticalFlow data.
public class optFlowTrackerFoxSimple extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, Serializable.Mat> optFlowMap;
    private HashMap<Integer, Queue<Object>> traceQueue;

    private int taskIndex;
    private int taskCnt;

    String traceAggBoltNameString;
    List<Integer> traceAggBoltTasks;

    public optFlowTrackerFoxSimple(String traceAggBoltNameString) {
        this.traceAggBoltNameString = traceAggBoltNameString;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        optFlowMap = new HashMap<>();
        traceQueue = new HashMap<>();

        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        this.traceAggBoltTasks = topologyContext.getComponentTasks(traceAggBoltNameString);
    }

    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage fake = new opencv_core.IplImage();

        if (streamId.equals(STREAM_NEW_TRACE)) {
            List<NewTraceMeta> traces = (List<NewTraceMeta>) tuple.getValueByField(FIELD_TRACE_META_LAST_POINT);
            traceQueue.computeIfAbsent(frameId, k -> new LinkedList<>()).addAll(traces);
            if (optFlowMap.containsKey(frameId)) {
                processTraceRecords(frameId);
            }
        }else if (streamId.equals(STREAM_NEW_TRACE) || streamId.equals(STREAM_RENEW_TRACE)) {
            List<TraceMetaAndLastPoint> traces = (List<TraceMetaAndLastPoint>) tuple.getValueByField(FIELD_TRACE_META_LAST_POINT);
            traceQueue.computeIfAbsent(frameId, k -> new LinkedList<>()).addAll(traces);
            if (optFlowMap.containsKey(frameId)) {
                processTraceRecords(frameId);
            }
        }else if (streamId.equals(STREAM_OPT_FLOW)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            optFlowMap.computeIfAbsent(frameId, k->sMat);
            if (traceQueue.containsKey(frameId)) {
                processTraceRecords(frameId);
            }
        } else if (streamId.equals(STREAM_CACHE_CLEAN)) {
            optFlowMap.remove(frameId);
            traceQueue.remove(frameId);
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_EXIST_REMOVE_TRACE, true, new Fields(FIELD_FRAME_ID, FIELD_TRACE_CONTENT));
    }

    public void processTraceRecords(int frameId) {
        Serializable.Mat sMat = optFlowMap.get(frameId);
        opencv_core.Mat orgMat = sMat.toJavaCVMat();
        IplImage flow = orgMat.asIplImage();
        Queue<Object> traceRecords = traceQueue.get(frameId);

        List<List<Object>> messages = new ArrayList<>();
        for (int i = 0; i < this.traceAggBoltTasks.size(); i ++){
            messages.add(new ArrayList<>());
        }
//        List<float[]> groups = new ArrayList<>();
//        for (int h = 0; h < flow.height(); h++) {
//            //FloatBuffer floatBuffer = flow.getByteBuffer(h * flow.widthStep()).asFloatBuffer();
//            FloatBuffer floatBuffer =  flow.getByteBuffer(h * flow.widthStep()).asFloatBuffer();
//            float[] floatArray = new float[flow.width()*2];
//            floatBuffer.get(floatArray);
//
//            if (h % this.taskCnt == this.taskIndex) {
//                groups.add(floatArray);
//            }
//        }
        while (!traceRecords.isEmpty()) {
            Object m = traceRecords.poll();

            if (m instanceof TraceMetaAndLastPoint) {
                TraceMetaAndLastPoint trace = (TraceMetaAndLastPoint)m;

                Serializable.CvPoint2D32f pointOut = getNextFlowPointSimple(flow, trace.lastPoint);
//                Serializable.CvPoint2D32f pointOut = getNextFlowPointSimple(groups, new TwoIntegers(flow.width(), flow.height()), trace.lastPoint);
                int index = trace.getTargetTaskIndex(this.traceAggBoltTasks);
                if (pointOut != null) {
                    TraceMetaAndLastPoint traceNext = new TraceMetaAndLastPoint(trace.traceID, pointOut);
                    messages.get(index).add(traceNext);
//                    System.out.println("fID: " + frameId + ", (" + trace.lastPoint.x() + "," +trace.lastPoint.y()
//                            + ")->(" + + pointOut.x() + "," + pointOut.y() + ")");
                } else {
                    messages.get(index).add(trace.traceID);
                }
            } else if (m instanceof NewTraceMeta) {
                NewTraceMeta trace = (NewTraceMeta) m;
                Serializable.CvPoint2D32f pointOut = getNextFlowPointSimple(flow, trace.firstPoint);
//                Serializable.CvPoint2D32f pointOut = getNextFlowPointSimple(groups, new TwoIntegers(flow.width(), flow.height()), trace.firstPoint);
                int index = trace.getTargetTaskIndex(this.traceAggBoltTasks);
                if (pointOut != null) {
                    NewTraceMeta traceNext = new NewTraceMeta(trace.traceID, trace.firstPoint, pointOut);
                    messages.get(index).add(traceNext);
//                    System.out.println("fID: " + frameId + ", (" + trace.firstPoint.x() + "," +trace.firstPoint.y()
//                            + ")->(" + + pointOut.x() + "," + pointOut.y() + ")");
                } else {
                    messages.get(index).add(trace.traceID);
                }
            }
        }
        for (int i = 0; i < this.traceAggBoltTasks.size(); i ++){
            int tID = this.traceAggBoltTasks.get(i);
            collector.emitDirect(tID, STREAM_EXIST_REMOVE_TRACE, new Values(frameId, messages.get(i)));
        }
    }

    public Serializable.CvPoint2D32f getNextFlowPoint(IplImage flow, Serializable.CvPoint2D32f point_in) {

        int width = flow.width();
        int height = flow.height();

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

        Serializable.CvPoint2D32f offset = new Serializable.CvPoint2D32f();
        offset.x(xs.getLast());
        offset.y(ys.getLast());

        Serializable.CvPoint2D32f point_out = new Serializable.CvPoint2D32f();
        point_out.x(point_in.x() + offset.x());
        point_out.y(point_in.y() + offset.y());

        if (point_out.x() > 0 && point_out.x() < width && point_out.y() > 0 && point_out.y() < height) {
            return point_out;
        } else {
            return null;
        }
    }

    public Serializable.CvPoint2D32f getNextFlowPointSimple(IplImage flow, Serializable.CvPoint2D32f point_in) {

        int width = flow.width();
        int height = flow.height();

        //TODO:!!!!be careful!!!
        int p = Math.min(Math.max(cvFloor(point_in.x()), 0), width - 1);
        int q = Math.min(Math.max(cvFloor(point_in.y()), 0), height - 1);

        //int p = Math.min(Math.max(cvRound(point_in.x()), 0), width - 1);
        //int q = Math.min(Math.max(cvRound(point_in.y()), 0), height - 1);

        FloatBuffer floatBuffer = flow.getByteBuffer(q * flow.widthStep()).asFloatBuffer();
        int xsIndex = 2 * p;
        int ysIndex = 2 * p + 1;

        Serializable.CvPoint2D32f point_out = new Serializable.CvPoint2D32f();
        point_out.x(point_in.x() + floatBuffer.get(xsIndex));
        point_out.y(point_in.y() + floatBuffer.get(ysIndex));

//        System.out.println("(" + point_in.x() + "," +point_in.y() + "," + p + "," + q + "," + xsIndex + "," + ysIndex
//                 + "," +  floatBuffer.get(xsIndex) + "," + floatBuffer.get(ysIndex) + ")->(" + + point_out.x() + "," + point_out.y() + ")");

        if (point_out.x() > 0 && point_out.x() < width && point_out.y() > 0 && point_out.y() < height) {
            return point_out;
        } else {
            return null;
        }
    }

    public Serializable.CvPoint2D32f getNextFlowPointSimple(List<float[]> floatArray, TwoIntegers whInfo, Serializable.CvPoint2D32f point_in) {

        int width = whInfo.getV1();
        int height = whInfo.getV2();

        //TODO:!!!!be careful!!!
        int p = Math.min(Math.max(cvFloor(point_in.x()), 0), width - 1);
        int q = Math.min(Math.max(cvFloor(point_in.y()), 0), height - 1);

//        int p = Math.min(Math.max(cvRound(point_in.x()), 0), width - 1);
//        int q = Math.min(Math.max(cvRound(point_in.y()), 0), height - 1);

        if (q % this.taskCnt != this.taskIndex){
            System.out.println("Caution!!! q % this.taskCnt != this.taskIndex!, q: " + q + ", taskCnt: "  +taskCnt + ", taskIndex: " + this.taskIndex);
        }

        int rowIndex = q / this.taskCnt;
        float[] fData = floatArray.get(rowIndex);
        //FloatBuffer floatBuffer = ByteBuffer.wrap(data).asFloatBuffer();

        //FloatBuffer floatBuffer = flow.getByteBuffer(q * flow.widthStep()).asFloatBuffer();
        int xsIndex = 2 * p;
        int ysIndex = 2 * p + 1;

        Serializable.CvPoint2D32f point_out = new Serializable.CvPoint2D32f();
//        point_out.x(point_in.x() + floatBuffer.get(xsIndex));
//        point_out.y(point_in.y() + floatBuffer.get(ysIndex));
        point_out.x(point_in.x() + fData[xsIndex]);
        point_out.y(point_in.y() + fData[ysIndex]);


        if (point_out.x() > 0 && point_out.x() < width && point_out.y() > 0 && point_out.y() < height) {
            return point_out;
        } else {
            return null;
        }
    }
}

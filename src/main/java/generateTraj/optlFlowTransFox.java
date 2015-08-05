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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * Shall be in charge the whole frame!!!
 */
public class optlFlowTransFox extends BaseRichBolt {
    OutputCollector collector;

    List<Integer> targetComponentTasks;
    String flowTrackerName;

    public optlFlowTransFox(String flowTrackerName) {
        this.flowTrackerName = flowTrackerName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        targetComponentTasks = topologyContext.getComponentTasks(flowTrackerName);

        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        opencv_core.Mat orgMat = sMat.toJavaCVMat();
        opencv_core.IplImage flow = orgMat.asIplImage();

        List<List<float[]>> group = new ArrayList<>();

        for (int i = 0; i < targetComponentTasks.size(); i++) {
            List<float[]> subGroup = new ArrayList<>();
            group.add(subGroup);
        }

        int height = flow.height();
        int width = flow.width();
        for (int h = 0; h < height; h++) {
            //FloatBuffer floatBuffer = flow.getByteBuffer(h * flow.widthStep()).asFloatBuffer();
            FloatBuffer floatBuffer =  flow.getByteBuffer(h * flow.widthStep()).asFloatBuffer();
            float[] floatArray = new float[width*2];
            floatBuffer.get(floatArray);

            int index = h % targetComponentTasks.size();
            group.get(index).add(floatArray);
        }

        //collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat));

        for (int i = 0; i < targetComponentTasks.size(); i++) {
            int tID = targetComponentTasks.get(i);
            collector.emitDirect(tID, STREAM_OPT_FLOW, tuple, new Values(frameId, group.get(i), new TwoIntegers(flow.width(), flow.height())));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW, true, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_WIDTH_HEIGHT));
    }
}

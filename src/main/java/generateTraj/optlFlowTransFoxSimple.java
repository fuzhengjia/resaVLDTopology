package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tool.Serializable;

import java.util.List;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class optlFlowTransFoxSimple extends BaseRichBolt {
    OutputCollector collector;
    private int taskIndex;
    private int taskCnt;
    List<Integer> targetComponentTasks;
    String flowTrackerName;

    public optlFlowTransFoxSimple(String flowTrackerName) {
        this.flowTrackerName = flowTrackerName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        targetComponentTasks = topologyContext.getComponentTasks(flowTrackerName);
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        //collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat));

        for (int i = 0; i < targetComponentTasks.size(); i ++) {
            int tID = targetComponentTasks.get(i);
            if (tID % this.taskCnt == this.taskIndex) {
                collector.emitDirect(tID, STREAM_OPT_FLOW, tuple, new Values(frameId, sMat));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

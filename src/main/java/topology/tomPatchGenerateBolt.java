package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

import static topology.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tomPatchGenerateBolt extends BaseRichBolt {
    OutputCollector collector;
    private int taskIndex;
    private int taskCnt;
    List<Integer> targetComponentTasks;
    //List<Integer> commonWorkerTasks;
    //List<Integer> localComponentTasks;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        targetComponentTasks = topologyContext.getComponentTasks("processor");
        //commonWorkerTasks = topologyContext.getThisWorkerTasks();
        //localComponentTasks = new ArrayList<Integer>(targetComponentTasks);
        //localComponentTasks.retainAll(commonWorkerTasks);
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField("frameId");
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField("frameMat");

        //TODO get params from config map
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int patchCount = 0;
        for (int x = 0; x + w <= W; x += dx)
            for (int y = 0; y + h <= H; y += dy)
                patchCount++;

        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                Serializable.PatchIdentifier identifier = new
                        Serializable.PatchIdentifier(frameId, new Serializable.Rect(x, y, w, h));
                collector.emit(PATCH_STREAM, tuple, new Values(identifier, patchCount));
            }
        }

        /*
        if (localComponentTasks.size() > 0) {
            for (int i = 0; i < localComponentTasks.size(); i++) {
                int tID = localComponentTasks.get(i);
                collector.emitDirect(tID, RAW_FRAME_STREAM, tuple, new Values(frameId, sMat, patchCount));
            }
        }
        */

        for (int i = 0; i < targetComponentTasks.size(); i ++) {
            int tID = targetComponentTasks.get(i);
            if (tID % this.taskCnt == this.taskIndex) {
                collector.emitDirect(tID, RAW_FRAME_STREAM, tuple, new Values(frameId, sMat, patchCount));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_STREAM, new Fields("patchIdentifier", "patchCount"));
        //EmitDirect
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, true, new Fields("frameId", "frameMat", "patchCount"));
    }
}

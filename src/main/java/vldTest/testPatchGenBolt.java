package vldTest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import topology.Serializable;

import java.util.List;
import java.util.Map;

import static topology.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class testPatchGenBolt extends BaseRichBolt {
    OutputCollector collector;
    private int taskIndex;
    private int taskCnt;
    List<Integer> targetComponentTasks;
    //List<Integer> commonWorkerTasks;
    //List<Integer> localComponentTasks;
    private String targetComponentName;

    public testPatchGenBolt(String targetComponentName){
        this.targetComponentName = targetComponentName;
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        targetComponentTasks = topologyContext.getComponentTasks(this.targetComponentName);
        //commonWorkerTasks = topologyContext.getThisWorkerTasks();
        //localComponentTasks = new ArrayList<Integer>(targetComponentTasks);
        //localComponentTasks.retainAll(commonWorkerTasks);
    }

    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();

        if (streamId.equals(RAW_FRAME_STREAM)) {

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

            int pCnt = 0;
            for (int x = 0; x + w <= W; x += dx) {
                for (int y = 0; y + h <= H; y += dy) {
                    if (pCnt % this.taskCnt == this.taskIndex) {
                        Serializable.PatchIdentifier identifier = new
                                Serializable.PatchIdentifier(frameId, new Serializable.Rect(x, y, w, h));
                        collector.emit(PATCH_STREAM, tuple, new Values(identifier, patchCount));
                    }
                    pCnt++;
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

            for (int i = 0; i < targetComponentTasks.size(); i++) {
                int tID = targetComponentTasks.get(i);
                if (tID % this.taskCnt == this.taskIndex) {
                    collector.emitDirect(tID, RAW_FRAME_STREAM, tuple, new Values(frameId, sMat, patchCount));
                }
            }
        } else if (streamId.equals(CACHE_CLEAR_STREAM)) {
            int frameId = tuple.getIntegerByField("frameId");
            for (int i = 0; i < targetComponentTasks.size(); i++) {
                int tID = targetComponentTasks.get(i);
                if (tID % this.taskCnt == this.taskIndex) {
                    collector.emitDirect(tID, CACHE_CLEAR_STREAM, tuple, new Values(frameId));
                }
            }
        } else  if (streamId.equals(LOGO_TEMPLATE_UPDATE_STREAM)) {
            for (int i = 0; i < targetComponentTasks.size(); i++) {
                int tID = targetComponentTasks.get(i);
                if (tID % this.taskCnt == this.taskIndex) {
                    collector.emitDirect(tID, LOGO_TEMPLATE_UPDATE_STREAM, tuple, new Values(tuple.getValues()));
                }
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_STREAM, new Fields("patchIdentifier", "patchCount"));
        //EmitDirect
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, true, new Fields("frameId", "frameMat", "patchCount"));
        outputFieldsDeclarer.declareStream(CACHE_CLEAR_STREAM, true, new Fields("frameId"));
        outputFieldsDeclarer.declareStream(LOGO_TEMPLATE_UPDATE_STREAM, true,
                new Fields("hostPatchIdentifier", "detectedLogoRect", "parentIdentifier"));
    }
}

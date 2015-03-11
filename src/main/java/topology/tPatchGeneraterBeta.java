package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static topology.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tPatchGeneraterBeta extends BaseRichBolt {
    OutputCollector collector;
    private int taskIndex;
    private int taskCnt;
    List<Integer> targetComponentTasks;
    String targetComponentName;

    public tPatchGeneraterBeta(String targetComponentName) {
        this.targetComponentName = targetComponentName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        targetComponentTasks = topologyContext.getComponentTasks(targetComponentName);

        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);

        //TODO get params from config map
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int patchCount = 0;
        int xCnt = 0;
        int yCnt = 0;
        for (int x = 0; x + w <= W; x += dx) {
            xCnt++;
        }
        for (int y = 0; y + h <= H; y += dy) {
            yCnt++;
        }

        List<List<Serializable.PatchIdentifierMat>> newPatches = new ArrayList<>();
        for (int i = 0; i < targetComponentTasks.size(); i++) {
            newPatches.add(new ArrayList<>());
        }

        int pIndex = 0;
        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                if (pIndex % this.taskCnt == this.taskIndex) {
                    Serializable.Rect rect = new Serializable.Rect(x, y, w, h);
                    opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
                    Serializable.Mat pSMat = new Serializable.Mat(pMat);
                    Serializable.PatchIdentifierMat identifierMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);

                    int index = pIndex % targetComponentTasks.size();
                    newPatches.get(index).add(identifierMat);
                }
                pIndex++;
            }
        }

        for (int i = 0; i < targetComponentTasks.size(); i++) {
            int tID = targetComponentTasks.get(i);
            collector.emitDirect(tID, PATCH_FRAME_STREAM, tuple, new Values(frameId, newPatches.get(i), pIndex));
            System.out.println("sendTo tID: " + tID + ", patchSize: " + newPatches.get(i).size() + ", totalPCnt: " + pIndex);
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, true, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
    }
}

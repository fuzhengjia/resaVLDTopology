package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.util.List;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tPatchGeneraterBeta extends BaseRichBolt {
    OutputCollector collector;
    private int thisTaskIndex;
    private int thisTaskCnt;
    List<Integer> targetComponentTasks;
    String targetComponentName;
    int targetTaskCnt;

    public tPatchGeneraterBeta(String targetComponentName) {
        this.targetComponentName = targetComponentName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.thisTaskIndex = topologyContext.getThisTaskIndex();
        this.thisTaskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        targetComponentTasks = topologyContext.getComponentTasks(targetComponentName);
        this.targetTaskCnt = targetComponentTasks.size();
    }

    @Override
    public void execute(Tuple tuple) {

        opencv_core.IplImage fk = new opencv_core.IplImage();

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);

        //TODO get params from config map
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;//7*7
        //double fsx = .4, fsy = .4;//8*8

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int xCnt = 0;
        int yCnt = 0;
        for (int x = 0; x + w <= W; x += dx) {
            xCnt++;
        }
        for (int y = 0; y + h <= H; y += dy) {
            yCnt++;
        }

//        List<List<Serializable.PatchIdentifierMat>> newPatches = new ArrayList<>();
//        for (int i = 0; i < targetComponentTasks.size(); i++) {
//            newPatches.add(new ArrayList<>());
//        }

        int patchCount = 0;
        int pIndex = xCnt * yCnt;
        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                patchCount ++;
                if (patchCount % this.thisTaskCnt == this.thisTaskIndex) {
                    Serializable.Rect rect = new Serializable.Rect(x, y, w, h);
                    opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
                    Serializable.Mat pSMat = new Serializable.Mat(pMat);
                    Serializable.PatchIdentifierMat patchIdentifierMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);


                    collector.emit(PATCH_FRAME_STREAM, tuple, new Values(frameId, patchIdentifierMat, pIndex));
//                    int index = pIndex % targetComponentTasks.size();
//                    newPatches.get(index).add(patchIdentifierMat);
                }
                //pIndex++;
            }
        }

//        for (int i = 0; i < targetComponentTasks.size(); i++) {
//            int tID = targetComponentTasks.get(i);
//            if (newPatches.get(i).size() > 0) {
//                collector.emitDirect(tID, PATCH_FRAME_STREAM, tuple, new Values(frameId, newPatches.get(i), pIndex));
//                System.out.println("sendTo tID: " + tID + ", patchSize: " + newPatches.get(i).size() + ", totalPCnt: " + pIndex);
//            } else {
//                System.out.println("nothing to tID: " + tID + ", patchSize: " + newPatches.get(i).size() + ", totalPCnt: " + pIndex);
//            }
//        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, true, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
    }
}

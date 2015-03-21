package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import generateTraj.TwoIntegers;
import org.bytedeco.javacpp.opencv_core;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static tool.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tPatchGeneraterGamma extends BaseRichBolt {
    OutputCollector collector;
    private int thisTaskIndex;
    private int thisTaskCnt;
    List<Integer> targetComponentTasks;
    private int targetTaskCount;
    String targetComponentName;
    int sendingIndex;

    public tPatchGeneraterGamma(String targetComponentName) {
        this.targetComponentName = targetComponentName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.thisTaskIndex = topologyContext.getThisTaskIndex();
        this.thisTaskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        //Each generator only send to its designated target task
        targetComponentTasks = topologyContext.getComponentTasks(targetComponentName).stream().
                filter(id -> id % this.thisTaskCnt == this.thisTaskIndex).collect(Collectors.toList());
        targetTaskCount = targetComponentTasks.size();
        this.sendingIndex = 0;
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.PatchIdentifierMat identifierMat = (Serializable.PatchIdentifierMat) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);
        TwoIntegers wh = (TwoIntegers)tuple.getValueByField(FIELD_WIDTH_HEIGHT);

        //TODO get params from config map
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;

        int W = wh.getV1(), H = wh.getV2();

        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);

        int yCnt = 0;
        Serializable.PatchIdentifier patchIdentifier = identifierMat.identifier;
        opencv_core.Mat subMat = identifierMat.sMat.toJavaCVMat();
        int currentHeight = patchIdentifier.roi.height;
        for (int y = 0; y + h <= currentHeight; y += dy) {
            yCnt++;
            for (int x = 0; x + w <= W; x += dx) {
                Serializable.Rect rect = new Serializable.Rect(x, y, w, h);
                opencv_core.Mat pMat = new opencv_core.Mat(subMat, rect.toJavaCVRect());
                Serializable.Mat pSMat = new Serializable.Mat(pMat);
                Serializable.Rect adjRect = new Serializable.Rect(x, y + patchIdentifier.roi.y, w, h);
                Serializable.PatchIdentifierMat patchMat = new Serializable.PatchIdentifierMat(frameId, adjRect, pSMat);

                int tID = targetComponentTasks.get(this.sendingIndex % targetTaskCount);
                collector.emitDirect(tID, PATCH_FRAME_STREAM, tuple, new Values(frameId, patchMat));
                if (++this.sendingIndex > 10000){
                    this.sendingIndex = 0;
                }
            }
        }
        System.out.println("getSub: " + patchIdentifier.toString() + ", yCnt: " + yCnt);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, true, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT));
    }
}

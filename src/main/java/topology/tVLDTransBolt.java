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

import static org.bytedeco.javacpp.opencv_core.cvMat;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static tool.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tVLDTransBolt extends BaseRichBolt {
    OutputCollector collector;

    List<Integer> targetComponentTasks;
    String targetComponentName;

    public tVLDTransBolt(String targetComponentName){
        this.targetComponentName = targetComponentName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);

        collector.emit(RAW_FRAME_STREAM, tuple, new Values(frameId, sMat));

        //TODO get params from config map
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;
        //double fsx = .4, fsy = .4;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);

//        int xCnt = 0;
        int blockCnt = 4;
        int yCnt = 0;
//        for (int x = 0; x + w <= W; x += dx) {
//            xCnt++;
//        }
        for (int y = 0; y + h <= H; y += dy) {
            yCnt++;
        }

        int step = (yCnt - 1) / blockCnt + 1;
        TwoIntegers wh = new TwoIntegers(W, H);

        for (int i = 0; i < blockCnt; i ++){

            int yy = i * step * dy;
            int hh = Math.min((step - 1) * dy + h, H - yy);

            Serializable.Rect rect = new Serializable.Rect(0, yy, W, hh);
            opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
            Serializable.Mat pSMat = new Serializable.Mat(pMat);
            Serializable.PatchIdentifierMat identifierMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);

            collector.emit(PATCH_FRAME_STREAM, tuple, new Values(frameId, identifierMat, wh));
            System.out.println("send out, yy: " + yy + ", hh: " + hh);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_WIDTH_HEIGHT));
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

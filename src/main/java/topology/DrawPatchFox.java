package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Util;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Tom Fu at Mar 24, 2015
 * This bolt is designed to draw found rects on each frame,
 * support for multiple target logos //todo: merge to parameter setting instead of current hard coding
 */
public class DrawPatchFox extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, List<List<Serializable.Rect>>> foundRectList;
    private HashMap<Integer, Serializable.Mat> frameMap;

    List<opencv_core.CvScalar> colorList;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

        frameMap = new HashMap<>();
        foundRectList = new HashMap<>();

        colorList = new ArrayList<>();
        colorList.add(opencv_core.CvScalar.MAGENTA);
        colorList.add(opencv_core.CvScalar.YELLOW);
        colorList.add(opencv_core.CvScalar.CYAN);
        colorList.add(opencv_core.CvScalar.BLUE);
        colorList.add(opencv_core.CvScalar.GREEN);
        colorList.add(opencv_core.CvScalar.RED);
        colorList.add(opencv_core.CvScalar.BLACK);
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        if (streamId.equals(PROCESSED_FRAME_STREAM)) {
            List<List<Serializable.Rect>> list = (List<List<Serializable.Rect>>) tuple.getValueByField(FIELD_FOUND_RECT_LIST);
            foundRectList.put(frameId, list);
        } else if (streamId.equals(RAW_FRAME_STREAM)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            frameMap.put(frameId, sMat);
        }

        if (frameMap.containsKey(frameId) && foundRectList.containsKey(frameId)) {
            opencv_core.Mat mat = frameMap.get(frameId).toJavaCVMat();
            List<List<Serializable.Rect>> list = foundRectList.get(frameId);

            for (int logoIndex = 0; logoIndex < list.size(); logoIndex ++) {

                opencv_core.CvScalar color = colorList.get(logoIndex % colorList.size());
                if (list.get(logoIndex) != null) {
                    for (Serializable.Rect rect : list.get(logoIndex)) {
                        Util.drawRectOnMat(rect.toJavaCVRect(), mat, color);
                    }
                }
            }

            Serializable.Mat sMatNew = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMatNew));

            System.out.println("FrameDisplay-finishedAdd: " + frameId +"@" + System.currentTimeMillis());
            foundRectList.remove(frameId);
            frameMap.remove(frameId);
        } else {
            //System.out.println("tDrawPatchBetaFinished: " + System.currentTimeMillis() + ":" + frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

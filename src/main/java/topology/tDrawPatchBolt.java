package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Debug;
import logodetection.Util;
import org.bytedeco.javacpp.opencv_core;
import tool.RedisStreamProducerBeta;
import util.ConfigUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tDrawPatchBolt extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducerBeta producer;

    //int lim = 31685; // SONY
    private HashMap<Integer, List<Serializable.Rect>> foundRectList;
    private HashMap<Integer, Serializable.Mat> frameMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

        frameMap = new HashMap<>();
        foundRectList = new HashMap<>();
    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        if (streamId.equals(PROCESSED_FRAME_STREAM)) {
            List<Serializable.Rect> list = (List<Serializable.Rect>) tuple.getValueByField(FIELD_FOUND_RECT_LIST);
            foundRectList.computeIfAbsent(frameId, k->list);
        } else if (streamId.equals(RAW_FRAME_STREAM)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            frameMap.computeIfAbsent(frameId, k->sMat);
        }

        if (frameMap.containsKey(frameId) && foundRectList.containsKey(frameId)) {
            opencv_core.Mat mat = frameMap.get(frameId).toJavaCVMat();
            List<Serializable.Rect> list = foundRectList.get(frameId);

            if (list != null) {
                for (Serializable.Rect rect : list) {
                    Util.drawRectOnMat(rect.toJavaCVRect(), mat, opencv_core.CvScalar.MAGENTA);
                }
            }

            Serializable.Mat sMatNew = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMatNew));

            System.out.println("FrameDisplay-finishedAdd: " + frameId +"@" + System.currentTimeMillis());
            foundRectList.remove(frameId);
            frameMap.remove(frameId);
        } else {
            System.out.println("tDrawPatchBetaFinished: " + System.currentTimeMillis() + ":" + frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

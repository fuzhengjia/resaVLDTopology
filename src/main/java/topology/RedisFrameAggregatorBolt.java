package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import logodetection.Debug;
import logodetection.Util;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.PROCESSED_FRAME_STREAM;
import static tool.Constants.RAW_FRAME_STREAM;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameAggregatorBolt extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducer producer;

    //int lim = 31685; // SONY
    private HashMap<Integer, List<Serializable.Rect> > processedFrames;
    private HashMap<Integer, Serializable.Mat> frameMap;

    private String host;
    private int port;
    private String queueName;
    private long count;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        host = getString(map, "redis.host");
        port = getInt(map, "redis.port");
        queueName = getString(map, "redis.queueName");
        processedFrames = new HashMap<>();
        frameMap = new HashMap<>();
        this.collector = outputCollector;
        producer = new RedisStreamProducer(host, port, queueName);
        new Thread(producer).start();
        count = 0;

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField("frameId");
        if (streamId.equals(PROCESSED_FRAME_STREAM)) {
            //if (frameId == firstFrameId)
            //    System.out.println("FIRST_FRAME=" + System.currentTimeMillis());
            //else if (frameId == lastFrameId - 1)
            //    System.out.println("LAST_FRAME=" + System.currentTimeMillis());

            List<Serializable.Rect> list = (List<Serializable.Rect>)tuple.getValueByField("foundRectList");
            opencv_core.Mat mat = null;
            if (frameMap.containsKey(frameId)) {
                mat = frameMap.get(frameId).toJavaCVMat();

                if (Debug.topologyDebugOutput)
                    System.out.println("Frame " + frameId + " received " + (list == null ? 0 : list.size()) + " logos were found");

                //TODO: change here for adding detection rectangle
                if (list != null) {
                    for (Serializable.Rect rect : list) {
                        Util.drawRectOnMat(rect.toJavaCVRect(), mat, opencv_core.CvScalar.MAGENTA);
                    }
                }
                if (Debug.topologyDebugOutput)
                    System.out.println("Frame " + frameId + " submitted to producer");
                producer.addFrame(new StreamFrame(frameId, mat));
                //System.out.println("finishedAdd: " + System.currentTimeMillis() + ":" + frameId);
                frameMap.remove(frameId);
            } else {
                processedFrames.put(frameId, list);
            }

        } else if (streamId.equals(RAW_FRAME_STREAM)) {
            //int frameId = tuple.getIntegerByField("frameId");
            if (frameMap.containsKey(frameId)) {
                if (Debug.topologyDebugOutput)
                    System.err.println("FrameAggregator: Received duplicate message");
            } else {
                Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField("frameMat");
                if (processedFrames.containsKey(frameId)) {
                    opencv_core.Mat mat = sMat.toJavaCVMat();

                    List<Serializable.Rect> list = processedFrames.get(frameId);
                    if (Debug.topologyDebugOutput)
                        System.out.println("Frame " + frameId + " received " + (list == null ? 0 : list.size()) + " logos were found");
                    if (list != null) {
                        for (Serializable.Rect rect : list) {
                            Util.drawRectOnMat(rect.toJavaCVRect(), mat, opencv_core.CvScalar.MAGENTA);
                        }
                    }
                    producer.addFrame(new StreamFrame(frameId, mat));
                    //System.out.println("finishedAdd: " + System.currentTimeMillis() + ":" + frameId);
                    processedFrames.remove(frameId);
                    frameMap.remove(frameId);
                } else {
                    frameMap.put(frameId, sMat);
                }
            }
        }

        //System.out.println("finished: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

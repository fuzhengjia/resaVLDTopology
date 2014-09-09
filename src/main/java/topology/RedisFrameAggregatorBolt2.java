package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import logodetection.Debug;
import logodetection.Util;
import org.bytedeco.javacpp.opencv_core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static topology.Constants.PROCESSED_FRAME_STREAM;
import static topology.Constants.RAW_FRAME_STREAM;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameAggregatorBolt2 extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducer producer;

    //int lim = 31685; // SONY
    private HashMap<Integer, List<Serializable.Rect>> processedFrames;
    private HashMap<Integer, Serializable.Mat> frameMap;
    private List<Serializable.Rect> listHistory;

    private String host;
    private int port;
    private String queueName;
    private long count;
    private int persistFrames;
    private int lastChangedFrame;
    private boolean firstChange;

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
        persistFrames = Math.max(getInt(map, "persistFrames"), 1);
        lastChangedFrame = 0;
        firstChange = true;
        listHistory = null;
    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField("frameId");

        if (streamId.equals(PROCESSED_FRAME_STREAM)) {
            List<Serializable.Rect> list = (List<Serializable.Rect>) tuple.getValueByField("foundRectList");
            if (!processedFrames.containsKey(frameId)) {
                processedFrames.put(frameId, list);
                //System.out.println("addtoProcessedFrames: " + System.currentTimeMillis() + ":" + frameId);
            }
        } else if (streamId.equals(RAW_FRAME_STREAM)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField("frameMat");
            if (!frameMap.containsKey(frameId)) {
                frameMap.put(frameId, sMat);
                //System.out.println("addtoFrameMap: " + System.currentTimeMillis() + ":" + frameId);
            } else {
                if (Debug.topologyDebugOutput)
                    System.err.println("FrameAggregator: Received duplicate message");
            }
        }

        if (frameMap.containsKey(frameId) && processedFrames.containsKey(frameId)){
            opencv_core.Mat mat = frameMap.get(frameId).toJavaCVMat();
            List<Serializable.Rect> list = processedFrames.get(frameId);

            /*
            if (firstChange){
                listHistory = list;
                lastChangedFrame = frameId;
                firstChange = false;
                System.out.println("firstChange: " + System.currentTimeMillis() + ":" + frameId + ":" + persistFrames);
            } else {
                if (frameId - lastChangedFrame >= persistFrames){
                    listHistory = list;
                    lastChangedFrame = frameId;
                } else {
                    list = listHistory;
                }
            }*/

            if (frameId % persistFrames == 0) {
                listHistory = list;
            } else {
                list = listHistory;
            }

            if (list != null) {
                for (Serializable.Rect rect : list) {
                    Util.drawRectOnMat(rect.toJavaCVRect(), mat, opencv_core.CvScalar.MAGENTA);
                }
            }
            producer.addFrame(new StreamFrame(frameId, mat));
            System.out.println("finishedAdd: " + System.currentTimeMillis() + ":" + frameId);
            processedFrames.remove(frameId);
            frameMap.remove(frameId);
        }
        //System.out.println("finished: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

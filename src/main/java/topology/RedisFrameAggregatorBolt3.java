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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static topology.Constants.PROCESSED_FRAME_STREAM;
import static topology.Constants.RAW_FRAME_STREAM;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameAggregatorBolt3 extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducer producer;

    /** Map from frameId to the list of logos detected. Cleared by tryRemovingList(). */
    private HashMap<Integer, List<Serializable.Rect> > processedFrames;
    /** Map from frameId to the image matrix. Cleared when the frame is submitted to producer. */
    private HashMap<Integer, Serializable.Mat> frameMap;
    /** Keeps track on which frames has been already sent. Is not cleared. */
    private HashSet<Integer> sentFrames;

    private String host;
    private int port;
    private String queueName;
    private int persistFrames = 0;
    private int firstFrameId = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        host = getString(map, "redis.host");
        port = getInt(map, "redis.port");
        queueName = getString(map, "redis.queueName");
        // for how many following frames will the logo persist on the output video?
        persistFrames = getInt(map, "persistFrames");
        processedFrames = new HashMap<>();
        frameMap = new HashMap<>();
        sentFrames = new HashSet<>();

        this.collector = outputCollector;
        producer = new RedisStreamProducer(host, port, queueName);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();

        /* Pseudo Code of the old version without persistence:

            If we received the list of detected logos then
                if the image matrix for this frame already received
                    draw rectangles on it and submit to produced, to produce ordered stream
                else
                    store this list and wait when the image matrix for this frame will come
            otherwise we received the the image matrix for some frame
                if the list of detected logos for this frame has been received
                    draw this list on the image matrix of this frame and submit it to producer
                else
                    store this matrix and wait until the list of logos for this frame will come

           Modified version with each frame persisting for the following 5 frames:
            If we received the list of detected logos for the certain frame then
                try drawing and sending its frame and the next 5 frames to producer
                Also try removing them from map to keep memory consistent
            otherwise we received the the image matrix for some frame
                try drawing and sending its frame to producer
                Also try removing them from map to keep memory consistent

         */
        if (streamId.equals(PROCESSED_FRAME_STREAM))
        {
            int frameId = tuple.getIntegerByField("frameId");
            List<Serializable.Rect> list = (List<Serializable.Rect>) tuple.getValueByField("foundRectList");
            if (list != null) {
                processedFrames.put(frameId, list);
                for (int i = frameId; i <= frameId + persistFrames; i++) {
                    trySendingFrame(i);
                }
                for (int i = frameId; i <= frameId + persistFrames; i++) {
                    tryRemovingList(i);
                }
            }
        } else if (streamId.equals(RAW_FRAME_STREAM))
        {
            int frameId = tuple.getIntegerByField("frameId");
            if (frameMap.containsKey(frameId)) {
                if (Debug.topologyDebugOutput)
                    System.err.println("FrameAggregator: Received duplicate message");
            }
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField("frameMat");
            frameMap.put(frameId, sMat);
            trySendingFrame(frameId);
            tryRemovingList(frameId);
        }


        collector.ack(tuple);
    }

    /**
     * Tries to submit the frame to stream producer. Frame can be only submitted if the lists of rectangles
     * corresponding to this frame and {@value this.persistFrames} previous frames have been received by this bolt.
     * In this case all these lists of rectangles are painted on the matrix (if it's also been received) and sumbitted
     * to producer.
     * @param frameId
     * @return
     */
    private boolean trySendingFrame(int frameId) {
        if (sentFrames.contains(frameId))
            return true;
        if (!frameMap.containsKey(frameId))
            return false;
        for (int i = frameId - persistFrames; i <= frameId ; i ++) {
            if (!processedFrames.containsKey(i)) {
                return false;
            }
        }
        sentFrames.add(frameId);
        opencv_core.Mat mat = frameMap.get(frameId).toJavaCVMat();
        for (int i = frameId - persistFrames; i <= frameId ; i ++) {
            List<Serializable.Rect> rectangles = processedFrames.get(i);
            for (Serializable.Rect rect : rectangles) {
                Util.drawRectOnMat(rect.toJavaCVRect(), mat, opencv_core.CvScalar.MAGENTA);
            }
        }
        producer.addFrame(new StreamFrame(frameId, mat));
        frameMap.remove(frameId);
        return true;
    }

    /**
     * Tries to remove the list of rectangles detected on a particular frame from the map processedFrames. It can be
     * removed from the map only if its frame and the following {@value this.persistFrames} frames have been submitted
     * to the producer.
     * @param frameId - the id of the frame corresponding, which list to remove
     * @return true, if the list removed from map and false otherwise.
     */
    private boolean tryRemovingList(int frameId) {
        for (int i = frameId ; i <= frameId + persistFrames ; i ++)
            if (!sentFrames.contains(i))
                return false;
        processedFrames.remove(frameId);
        return true;
    }
}

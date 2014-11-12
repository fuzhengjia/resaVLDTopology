package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.BytePointer;
import topology.Serializable;
import topology.StreamFrame;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.CV_8UC1;
import static org.bytedeco.javacpp.opencv_core.cvMat;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;
import static showTraj.Constant.*;

import org.bytedeco.javacpp.opencv_core;
/**
 * Created by Intern04 on 5/8/2014.
 */
public class RedisFrameOutput extends BaseRichBolt {
    OutputCollector collector;

    RedisStreamProducer producer;

    private String host;
    private int port;
    private String queueName;
    private int accumulateFrameSize;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        host = getString(map, "redis.host");
        port = getInt(map, "redis.port");
        queueName = getString(map, "redis.queueName");
        this.collector = outputCollector;

        accumulateFrameSize = Math.max(getInt(map, "accumulateFrameSize"), 1);

        producer = new RedisStreamProducer(host, port, queueName, accumulateFrameSize);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        byte[] imgBytes = sMat.toString().getBytes();
        opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));
        opencv_core.Mat mat = sMat.toJavaCVMat();
        producer.addFrame(new StreamFrame(frameId, sMat.toJavaCVMat()));

        System.out.println("finishedAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}

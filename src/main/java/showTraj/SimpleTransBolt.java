package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.CV_8UC1;
import static org.bytedeco.javacpp.opencv_core.cvMat;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static tool.Constants.*;
import static topology.StormConfigManager.getString;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class SimpleTransBolt extends BaseRichBolt {
    OutputCollector collector;

    private ArrayList<ArrayList<Float>> frameTraj = new  ArrayList<ArrayList<Float>>();
    private ArrayList<ArrayList<Float>> frameTrajIpnut = new  ArrayList<ArrayList<Float>>();
    private BufferedReader reader;
    private ArrayList<int[]> groupColor;
    private int maxFrameID;
    private ArrayList<Integer> groupIDs;

    private String path;
    private int repeatCount;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        byte[] imgBytes = (byte[]) tuple.getValueByField(FIELD_FRAME_BYTES);

        opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));

        opencv_core.Mat mat = new opencv_core.Mat(image);
        //opencv_core.Mat matNew = new opencv_core.Mat();
        //opencv_core.Size size = new opencv_core.Size(640, 480);
        //opencv_imgproc.resize(matOrg, matNew, size);
        //opencv_imgproc.resize(mat, matNew, size);

        //Serializable.Mat sMat = new Serializable.Mat(matNew);
        Serializable.Mat sMat = new Serializable.Mat(mat);
        collector.emit(STREAM_FRAME_OUTPUT, tuple, new Values(frameId, sMat));
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;

import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class SimpleTransByteArrBolt extends BaseRichBolt {
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        byte[] data = (byte[]) tuple.getValueByField(FIELD_FRAME_BYTES);

        Serializable.Mat sMat = new Serializable.Mat(data);
        //opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));

        //opencv_core.Mat mat = new opencv_core.Mat(image);
        //opencv_core.Mat matNew = new opencv_core.Mat();
        //opencv_core.Size size = new opencv_core.Size(640, 480);
        //opencv_imgproc.resize(matOrg, matNew, size);
        //opencv_imgproc.resize(mat, matNew, size);

        //Serializable.Mat sMat = new Serializable.Mat(matNew);
        //Serializable.Mat sMat = new Serializable.Mat(mat);
        collector.emit(STREAM_FRAME_OUTPUT, tuple, new Values(frameId, sMat));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

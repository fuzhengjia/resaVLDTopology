package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_video;
import topology.Serializable;
import util.ConfigUtil;

import java.nio.FloatBuffer;
import java.util.LinkedList;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constant.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class traceInit extends BaseRichBolt {
    OutputCollector collector;

    IplImage grey, eig;
    IplImagePyramid eig_pyramid;

    double min_distance;
    double quality;

    int init_counter;

    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static int ixyScale = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.grey = null;
        this.eig = null;
        this.eig_pyramid = null;

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        if (frameId % init_counter != 0){ ///every init_counter frames, generate new dense points.
            collector.ack(tuple);
            return;
        }

        IplImage imageFK = new IplImage();
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        this.grey = sMat.toJavaCVMat().asIplImage();

        if (this.eig_pyramid == null && frameId == 0){
            this.eig_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(this.grey), 32, 1);
        }

        this.eig = cvCloneImage(eig_pyramid.getImage(ixyScale));
        int width = cvFloor(grey.width() / min_distance);
        int height = cvFloor(grey.height() / min_distance);

        double[] maxVal = new double[1];
        maxVal[0] = 0.0;
        opencv_imgproc.cvCornerMinEigenVal(grey, this.eig, 3, 3);
        cvMinMaxLoc(eig, null, maxVal, null, null, null);
        double threshold = maxVal[0] * quality;
        int offset = cvFloor(min_distance / 2.0);

        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                int x = cvFloor(j * min_distance + offset);
                int y = cvFloor(i * min_distance + offset);
                int traceID = i * height + j;

                FloatBuffer floatBuffer = eig.getByteBuffer(y * eig.widthStep()).asFloatBuffer();
                float ve = floatBuffer.get(x);

                if (ve > threshold) {
                    collector.emit(STREAM_NEW_TRACE, tuple, new Values(frameId, traceID, new float[]{x, y}));
                }
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW,
        //        new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_MBHX_MAT, FIELD_MBHY_MAT));
        outputFieldsDeclarer.declareStream(STREAM_NEW_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_POINT));
    }

}

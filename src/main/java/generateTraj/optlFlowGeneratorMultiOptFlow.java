package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_video;
import tool.Serializable;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class optlFlowGeneratorMultiOptFlow extends BaseRichBolt {
    OutputCollector collector;
    IplImage grey, prev_grey;

//    float scale_stride;
//    int scale_num;
//    DescInfo mbhInfo;
//
//    static int patch_size = 32;
//    static int nxy_cell = 2;
//    static int nt_cell = 3;
//    static float min_flow = 0.4f * 0.4f;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.grey = null;
        this.prev_grey = null;
//        mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        IplImage imageFK = new IplImage();
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        this.grey = sMat.toJavaCVMat().asIplImage();

        Serializable.Mat sMatPrev = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT_PREV);
        this.prev_grey = sMatPrev.toJavaCVMat().asIplImage();

        IplImage flow = cvCreateImage(cvGetSize(this.grey), IPL_DEPTH_32F, 2);

        opencv_video.cvCalcOpticalFlowFarneback(this.prev_grey, this.grey, flow,
                Math.sqrt(2.0) / 2.0, 5, 10, 2, 7, 1.5, opencv_video.OPTFLOW_FARNEBACK_GAUSSIAN);

        Mat fMat = new Mat(flow);
        Serializable.Mat sfMat = new Serializable.Mat(fMat);

        collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

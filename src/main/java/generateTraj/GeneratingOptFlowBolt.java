package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_video;
import topology.Serializable;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constant.*;

/**
 * Created by Tom Fu
 * Input is raw video frame, prev_raw video frame, in sMat byte[]
 * generate optFlow, can be paralellized,
 * Ouput is byte[] of rawFrame sMat and result optFlow sMat
 */
public class GeneratingOptFlowBolt extends BaseRichBolt {
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        IplImage imageFK = new IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        byte[] sMatBA = (byte[])tuple.getValueByField(FIELD_FRAME_MAT);
        byte[] sMatPrevBA = (byte[])tuple.getValueByField(FIELD_FRAME_MAT_PREV);

        Serializable.Mat sMat = new Serializable.Mat(sMatBA);
        opencv_core.IplImage grey = sMat.toJavaCVMat().asIplImage();

        Serializable.Mat sMatPrev = new Serializable.Mat(sMatPrevBA);
        opencv_core.IplImage prev_grey = sMatPrev.toJavaCVMat().asIplImage();

        IplImage flow = cvCreateImage(cvGetSize(grey), IPL_DEPTH_32F, 2);

        opencv_video.cvCalcOpticalFlowFarneback(prev_grey, grey, flow,
                Math.sqrt(2.0) / 2.0, 5, 10, 2, 7, 1.5, opencv_video.OPTFLOW_FARNEBACK_GAUSSIAN);

        Mat fMat = new Mat(flow);
        Serializable.Mat sfMat = new Serializable.Mat(fMat);
        byte[] combined = Serializable.Mat.toByteArray(sMat, sfMat);

        collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, combined));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

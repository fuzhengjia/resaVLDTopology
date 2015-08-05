package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_video;
import tool.Serializable;
import util.ConfigUtil;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frame, prev_raw video frame, in sMat byte[]
 * generate optFlow, can be paralellized,
 * Ouput is byte[] of rawFrame sMat and result optFlow sMat
 */
public class GeneratingOptFlowBolt extends BaseRichBolt {
    OutputCollector collector;

    IplImage image, prev_image, grey, prev_grey;
    IplImagePyramid grey_pyramid, prev_grey_pyramid;

    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static int ixyScale = 0;

    double min_distance;
    double quality;
    int init_counter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.image = null;
        this.prev_image = null;
        this.grey = null;
        this.prev_grey = null;

        this.grey_pyramid = null;
        this.prev_grey_pyramid = null;

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        IplImage imageFK = new IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        byte[] sMatBA = (byte[])tuple.getValueByField(FIELD_FRAME_MAT);
        byte[] sMatPrevBA = (byte[])tuple.getValueByField(FIELD_FRAME_MAT_PREV);

        Serializable.Mat sMat = new Serializable.Mat(sMatBA);
        opencv_core.IplImage frame = sMat.toJavaCVMat().asIplImage();

        Serializable.Mat sMatPrev = new Serializable.Mat(sMatPrevBA);
        opencv_core.IplImage framePrev = sMatPrev.toJavaCVMat().asIplImage();

        if (this.image == null || frameId == 1) { //only first time
            image = cvCreateImage(cvGetSize(frame), 8, 3);
            image.origin(frame.origin());

            prev_image = cvCreateImage(cvGetSize(frame), 8, 3);
            prev_image.origin(frame.origin());

            grey = cvCreateImage(cvGetSize(frame), 8, 1);
            grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);

            prev_grey = cvCreateImage(cvGetSize(frame), 8, 1);
            prev_grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);
        }

        cvCopy(frame, image, null);
        opencv_imgproc.cvCvtColor(image, grey, opencv_imgproc.CV_BGR2GRAY);
        grey_pyramid.rebuild(grey);

        IplImage grey_temp = cvCloneImage(grey_pyramid.getImage(ixyScale));

        cvCopy(framePrev, prev_image, null);
        opencv_imgproc.cvCvtColor(prev_image, prev_grey, opencv_imgproc.CV_BGR2GRAY);
        prev_grey_pyramid.rebuild(prev_grey);

        IplImage prev_grey_temp = cvCloneImage(prev_grey_pyramid.getImage(ixyScale));

        IplImage flow = cvCreateImage(cvGetSize(grey), IPL_DEPTH_32F, 2);

        opencv_video.cvCalcOpticalFlowFarneback(prev_grey_temp, grey_temp, flow,
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

package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_video;
import topology.Serializable;

import java.nio.FloatBuffer;
import java.util.Map;

import static generateTraj.helperFunctions.HogComp;
import static generateTraj.helperFunctions.MbhComp;
import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class optlFlowGeneratorMultiOptFlowHogMBHSimple extends BaseRichBolt {
    OutputCollector collector;
    //IplImage grey, prev_grey;

//    float scale_stride;
//    int scale_num;

    DescInfo mbhInfo;
    DescInfo hogInfo;
//
    static int patch_size = 32;
    static int nxy_cell = 2;
    static int nt_cell = 3;
    static float min_flow = 0.4f * 0.4f;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        //this.grey = null;
        //this.prev_grey = null;
        hogInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
        mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        IplImage imageFK = new IplImage();
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        //this.grey = sMat.toJavaCVMat().asIplImage();
        //IplImage grey_temp = cvCloneImage(this.grey);
        IplImage grey_temp = cvCloneImage(sMat.toJavaCVMat().asIplImage());

        Serializable.Mat sMatPrev = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT_PREV);
        //this.prev_grey = sMatPrev.toJavaCVMat().asIplImage();
        //IplImage prev_grey_temp = cvCloneImage(this.prev_grey);
        IplImage prev_grey_temp = cvCloneImage(sMatPrev.toJavaCVMat().asIplImage());

        IplImage flow = cvCreateImage(cvGetSize(grey_temp), IPL_DEPTH_32F, 2);

        opencv_video.cvCalcOpticalFlowFarneback(prev_grey_temp, grey_temp, flow,
                Math.sqrt(2.0) / 2.0, 5, 10, 2, 7, 1.5, opencv_video.OPTFLOW_FARNEBACK_GAUSSIAN);

        Mat fMat = new Mat(flow);
        Serializable.Mat sfMat = new Serializable.Mat(fMat);

        collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat));

        int width = prev_grey_temp.width();
        int height = prev_grey_temp.height();
        DescMat[] mbhMatXY = MbhComp(flow, mbhInfo, width, height);
        DescMat mbhMatX = mbhMatXY[0];
        DescMat mbhMatY = mbhMatXY[1];
        DescMat hogMat = HogComp(prev_grey_temp, hogInfo, width, height);

        collector.emit(STREAM_FEATURE_FLOW, tuple, new Values(frameId, new DescMat[] {mbhMatX, mbhMatY, hogMat}));
        collector.ack(tuple);

        Serializable.CvPoint2D32f p = new Serializable.CvPoint2D32f();
        p.x(95.69521f);
        p.y(21.003756f);
        getNextFlowPointSimple(flow, p, frameId);

        cvRelease(grey_temp);
        cvRelease(prev_grey_temp);
        cvRelease(flow);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        outputFieldsDeclarer.declareStream(STREAM_FEATURE_FLOW, new Fields(FIELD_FRAME_ID, FIELD_MBH_HOG_MAT));
    }

    public Serializable.CvPoint2D32f getNextFlowPointSimple(IplImage flow, Serializable.CvPoint2D32f point_in, int fID) {

        int width = flow.width();
        int height = flow.height();

        //TODO:!!!!be careful!!!
        int p = Math.min(Math.max(cvFloor(point_in.x()), 0), width - 1);
        int q = Math.min(Math.max(cvFloor(point_in.y()), 0), height - 1);

        //int p = Math.min(Math.max(cvRound(point_in.x()), 0), width - 1);
        //int q = Math.min(Math.max(cvRound(point_in.y()), 0), height - 1);

        FloatBuffer floatBuffer = flow.getByteBuffer(q * flow.widthStep()).asFloatBuffer();
        int xsIndex = 2 * p;
        int ysIndex = 2 * p + 1;

        Serializable.CvPoint2D32f point_out = new Serializable.CvPoint2D32f();
        point_out.x(point_in.x() + floatBuffer.get(xsIndex));
        point_out.y(point_in.y() + floatBuffer.get(ysIndex));

        System.out.println("(" + point_in.x() + "," +point_in.y() + "," + p + "," + q + "," + xsIndex + "," + ysIndex
                + "," +  floatBuffer.get(xsIndex) + "," + floatBuffer.get(ysIndex) + ")->(" + + point_out.x() + "," + point_out.y() + ")");

        System.out.print("fID: " + fID + ", Line: " + q + ":");
        for (int t = 0; t < width - 1; t ++){
            System.out.print(floatBuffer.get(t * 2) + "," + floatBuffer.get(t * 2 + 1) + "->");
        }
        System.out.println();

        if (point_out.x() > 0 && point_out.x() < width && point_out.y() > 0 && point_out.y() < height) {
            return point_out;
        } else {
            return null;
        }
    }
}

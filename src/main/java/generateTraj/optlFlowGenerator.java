package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jmatio.io.MatFileReader;
import com.jmatio.types.MLCell;
import com.jmatio.types.MLDouble;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_video;
import topology.Serializable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_core.cvCopy;
import static org.bytedeco.javacpp.opencv_core.cvCreateImage;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static tool.Constant.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class optlFlowGenerator extends BaseRichBolt {
    OutputCollector collector;

    opencv_core.IplImage grey, prev_grey;

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
        opencv_core.IplImage imageFK = new opencv_core.IplImage();
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        this.grey = sMat.toJavaCVMat().asIplImage();

        if (this.prev_grey != null && frameId > 0){
            //for the first frame
            IplImage flow = cvCreateImage(cvGetSize(this.grey), IPL_DEPTH_32F, 2);

            opencv_video.cvCalcOpticalFlowFarneback(this.prev_grey, this.grey, flow,
                    Math.sqrt(2.0) / 2.0, 5, 10, 2, 7, 1.5, opencv_video.OPTFLOW_FARNEBACK_GAUSSIAN);

            opencv_core.Mat fMat = new opencv_core.Mat(flow);
            Serializable.Mat sfMat = new Serializable.Mat(fMat);

            //collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat, mbhMatX, mbhMatY));
            collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat));
        }

        this.prev_grey = cvCloneImage(this.grey);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

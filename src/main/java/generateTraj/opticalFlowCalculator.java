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
public class opticalFlowCalculator extends BaseRichBolt {
    OutputCollector collector;

    opencv_core.IplImage frame, image, prev_image, grey, prev_grey;
    IplImagePyramid grey_pyramid, prev_grey_pyramid;

    float scale_stride;
    int scale_num;

    DescInfo mbhInfo;

    static int patch_size = 32;
    static int nxy_cell = 2;
    static int nt_cell = 3;
    static float min_flow = 0.4f * 0.4f;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.frame = null;
        this.image = null;
        this.prev_image = null;
        this.grey = null;
        this.prev_grey = null;

        this.grey_pyramid = null;
        this.prev_grey_pyramid = null;

        scale_stride = (float) Math.sqrt(2.0);
        scale_num = 1;

        patch_size = 32;
        nxy_cell = 2;
        nt_cell = 3;
        min_flow = 0.4f * 0.4f;
        mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        frame = sMat.toJavaCVMat().asIplImage();

        if (this.image == null){
            //for the first frame
            image = cvCreateImage(cvGetSize(frame), 8, 3);
            image.origin(frame.origin());
            prev_image = cvCreateImage(cvGetSize(frame), 8, 3);
            prev_image.origin(frame.origin());

            grey = cvCreateImage(cvGetSize(frame), 8, 1);
            grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);
            prev_grey = cvCreateImage(cvGetSize(frame), 8, 1);
            prev_grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);

            cvCopy(frame, image, null);
            opencv_imgproc.cvCvtColor(image, grey, opencv_imgproc.CV_BGR2GRAY);
            grey_pyramid.rebuild(grey);
            cvCopy(frame, prev_image, null);
            opencv_imgproc.cvCvtColor(prev_image, prev_grey, opencv_imgproc.CV_BGR2GRAY);
            prev_grey_pyramid.rebuild(prev_grey);

        } else {
            ///for later frames
            cvCopy(frame, image, null);
            opencv_imgproc.cvCvtColor(image, grey, opencv_imgproc.CV_BGR2GRAY);
            grey_pyramid.rebuild(grey);

            ///we consider only one scale so far, so scale number = 0.
            IplImage prev_grey_temp = cvCloneImage(prev_grey_pyramid.getImage(0));
            IplImage grey_temp = cvCloneImage(grey_pyramid.getImage(0));

            IplImage flow = cvCreateImage(cvGetSize(grey_temp), IPL_DEPTH_32F, 2);

            opencv_video.cvCalcOpticalFlowFarneback(prev_grey_temp, grey_temp, flow,
                    Math.sqrt(2.0) / 2.0, 5, 10, 2, 7, 1.5, opencv_video.OPTFLOW_FARNEBACK_GAUSSIAN);

            int width = grey_temp.width();
            int height = grey_temp.height();
            DescMat[] mbhMatXY = MbhComp(flow, mbhInfo, width, height);
            DescMat mbhMatX = mbhMatXY[0];
            DescMat mbhMatY = mbhMatXY[1];

            opencv_core.Mat fMat = new opencv_core.Mat(flow);
            Serializable.Mat sfMat = new Serializable.Mat(fMat);

            collector.emit(STREAM_OPT_FLOW, tuple, new Values(frameId, sfMat, mbhMatX, mbhMatY));
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_OPT_FLOW,
                new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_MBHX_MAT, FIELD_MBHY_MAT));
    }

    //We have re-organized the input and output to the oringal c++ version
    public static DescMat[] MbhComp(IplImage flow, DescInfo descInfo,
                                    int width, int height) {
        //int width = descMatX.width;
        //int height = descMatX.height;
        IplImage flowX = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);
        IplImage flowY = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);
        IplImage flowXdX = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);
        IplImage flowXdY = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);
        IplImage flowYdX = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);
        IplImage flowYdY = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);

        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {

                FloatBuffer floatBuffer = flow.getByteBuffer(i * flow.widthStep()).asFloatBuffer();
                FloatBuffer floatBufferX = flowX.getByteBuffer(i * flowX.widthStep()).asFloatBuffer();
                FloatBuffer floatBufferY = flowY.getByteBuffer(i * flowY.widthStep()).asFloatBuffer();

                int fXIndex = j;
                int fYIndex = j;
                int fIndexForX = 2 * j;
                int fIndexForY = 2 * j + 1;

                floatBufferX.put(fXIndex, 100 * floatBuffer.get(fIndexForX));
                floatBufferY.put(fYIndex, 100 * floatBuffer.get(fIndexForY));
            }
        }

        opencv_imgproc.cvSobel(flowX, flowXdX, 1, 0, 1);
        opencv_imgproc.cvSobel(flowX, flowXdY, 0, 1, 1);
        opencv_imgproc.cvSobel(flowY, flowYdX, 1, 0, 1);
        opencv_imgproc.cvSobel(flowY, flowYdY, 0, 1, 1);

        DescMat[] retVal = new DescMat[2];
        retVal[0] = BuildDescMat(flowXdX, flowXdY, descInfo, width, height);//descMatX
        retVal[1] = BuildDescMat(flowYdX, flowYdY, descInfo, width, height);//descMatY

        cvReleaseImage(flowX);
        cvReleaseImage(flowY);
        cvReleaseImage(flowXdX);
        cvReleaseImage(flowXdY);
        cvReleaseImage(flowYdX);
        cvReleaseImage(flowYdY);

        return retVal;
    }

    //We have re-organized the input and output to the oringal c++ version
    public static DescMat BuildDescMat(IplImage xComp, IplImage yComp, DescInfo descInfo, int width, int height) {

        DescMat descMat = new DescMat(height, width, descInfo.nBins);
        // whether use full orientation or not
        float fullAngle = descInfo.fullOrientation > 0 ? 360 : 180;
        // one additional bin for hof
        int nBins = descInfo.flagThre > 0 ? descInfo.nBins - 1 : descInfo.nBins;

        float angleBase = fullAngle / (float) nBins;
        int histDim = descInfo.nBins;
        int index = 0;

        for (int i = 0; i < height; i++) {
            // the histogram accumulated in the current line
            float[] sum = new float[histDim];
            for (int j = 0; j < sum.length; j++) {
                sum[j] = 0;
            }
            for (int j = 0; j < width; j++, index++) {
                FloatBuffer floatBufferX = xComp.getByteBuffer(i * xComp.widthStep()).asFloatBuffer();
                FloatBuffer floatBufferY = yComp.getByteBuffer(i * yComp.widthStep()).asFloatBuffer();

                int xIndex = j;
                int yIndex = j;

                float shiftX = floatBufferX.get(xIndex);
                float shiftY = floatBufferY.get(yIndex);
                float magnitude0 = (float) Math.sqrt(shiftX * shiftX + shiftY * shiftY);
                float magnitude1 = magnitude0;
                int bin0, bin1;

                // for the zero bin of hof
                if (descInfo.flagThre == 1 && magnitude0 <= descInfo.threshold) {
                    bin0 = nBins; // the zero bin is the last one
                    magnitude0 = 1.0f;
                    bin1 = 0;
                    magnitude1 = 0;
                } else {
                    float orientation = cvFastArctan(shiftY, shiftX);
                    if (orientation > fullAngle) {
                        orientation -= fullAngle;
                    }

                    // split the magnitude to two adjacent bins
                    float fbin = orientation / angleBase;
                    bin0 = ((int) Math.floor(fbin + 0.5)) % nBins;
                    bin1 = ((fbin - bin0) > 0 ? (bin0 + 1) : (bin0 - 1 + nBins)) % nBins;

                    float weight0 = 1 - Math.min(Math.abs(fbin - bin0), nBins - fbin);
                    float weight1 = 1 - weight0;

                    magnitude0 *= weight0;
                    magnitude1 *= weight1;
                }

                sum[bin0] += magnitude0;
                sum[bin1] += magnitude1;

                int temp0 = index * descMat.nBins;
                if (i == 0) {
                    // for the 1st line
                    for (int m = 0; m < descMat.nBins; m++) {
                        descMat.desc[temp0++] = sum[m];
                    }
                } else {
                    int temp1 = (index - width) * descMat.nBins;
                    for (int m = 0; m < descMat.nBins; m++) {
                        descMat.desc[temp0++] = descMat.desc[temp1++] + sum[m];
                    }
                }
            }
        }
        return descMat;
    }
}

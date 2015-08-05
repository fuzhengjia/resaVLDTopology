package generateTraj;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.Serializable;

import java.io.*;
import java.nio.FloatBuffer;
import java.util.*;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_core.cvFastArctan;
import static org.bytedeco.javacpp.opencv_core.cvReleaseImage;

/**
 * Created by Tom.fu on 28/1/2015.
 */
public class helperFunctions {

    //We have re-organized the input and output to the oringal c++ version
    //i.e, we make points to be return values instead of an input.
    public static LinkedList<opencv_core.CvPoint2D32f> cvDenseSample(opencv_core.IplImage grey, opencv_core.IplImage eig,
                                                                     double quality, double min_distance) {

        LinkedList<opencv_core.CvPoint2D32f> points = new LinkedList<>();

        int width = cvFloor(grey.width() / min_distance);
        int height = cvFloor(grey.height() / min_distance);

        double[] maxVal = new double[1];
        maxVal[0] = 0.0;
        opencv_imgproc.cvCornerMinEigenVal(grey, eig, 3, 3);
        //TODO: here need to be careful check with original c++ code.
        //cvMinMaxLoc(eig, 0, &maxVal, 0, 0, 0);
        cvMinMaxLoc(eig, null, maxVal, null, null, null);
        double threshold = maxVal[0] * quality;


        int offset = cvFloor(min_distance / 2.0);
        //TODO:: is the calculation of "ve" correct?
        //FloatBuffer floatBuffer = eig.getFloatBuffer();
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                int x = cvFloor(j * min_distance + offset);
                int y = cvFloor(i * min_distance + offset);
                //int index = j * eig.widthStep() + eig.nChannels() * i;
                //int index = y * eig.widthStep() + x;
                //TODO:: caution, as explained by Peiyong, each row width is not dividable by size of float (4 bytes)
                //There fore, we need to go to the target row first, then get the col.
                FloatBuffer floatBuffer = eig.getByteBuffer(y * eig.widthStep()).asFloatBuffer();
                float ve = floatBuffer.get(x);

                if (ve > threshold) {
                    points.addLast(cvPoint2D32f(x, y));
                }
            }
        }

        return points;
    }

    //We have re-organized the input and output to the oringal c++ version
    //i.e, we make points to be return values instead of an input.
    public static LinkedList<opencv_core.CvPoint2D32f> cvDenseSample(opencv_core.IplImage grey, opencv_core.IplImage eig,
                                                                     LinkedList<opencv_core.CvPoint2D32f> points_in,
                                                                     double quality, double min_distance) {
        LinkedList<opencv_core.CvPoint2D32f> points_out = new LinkedList<>();
        int width = cvFloor(grey.width() / min_distance);
        int height = cvFloor(grey.height() / min_distance);

        double[] maxVal = new double[1];
        maxVal[0] = 0.0;
        opencv_imgproc.cvCornerMinEigenVal(grey, eig, 3, 3);
        //TODO: here need to be careful check with original c++ code.
        //cvMinMaxLoc(eig, 0, &maxVal, 0, 0, 0);
        cvMinMaxLoc(eig, null, maxVal, null, null, null);
        double threshold = maxVal[0] * quality;

//        System.out.println("cvDenseSample, w: " + width + ", h: " + height
//                + ", th: " + threshold + ", md: " + min_distance + ", pin.Size: " + points_in.size());

        int[] counters = new int[width * height];
        for (int i = 0; i < counters.length; i++) {
            counters[i] = 0;
        }
        for (int i = 0; i < points_in.size(); i++) {
            opencv_core.CvPoint2D32f point = points_in.get(i);

            int x = cvFloor(point.x() / min_distance);
            int y = cvFloor(point.y() / min_distance);
            int ywx = y * width + x;

            if (point.x() >= min_distance * width || point.y() >= min_distance * height) {
                //System.out.println("skip: i: " + i + ", ywx: " + ywx);
                continue;
            }
            counters[y * width + x]++;

        }


        int index = 0;
        int offset = cvFloor(min_distance / 2);
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++, index++) {
                if (counters[index] == 0) {
                    int x = cvFloor(j * min_distance + offset);
                    int y = cvFloor(i * min_distance + offset);

                    FloatBuffer floatBuffer = eig.getByteBuffer(y * eig.widthStep()).asFloatBuffer();
                    float ve = floatBuffer.get(x);
                    if (ve > threshold) {
//                        System.out.println("ve > th: index: " + index
//                                + ", i: " + i
//                                + ", j: " + j
//                                + ", x: " + x
//                                + ", y: " + y
//                                + ", ve: " + ve
//                                + ", threshold: " + threshold
//                                + ", counters: " + counters[index]
//                        );
                        points_out.addLast(cvPoint2D32f(x, y));
                    }else{
                    }
                }
            }
        }

        return points_out;
    }

    public static Object[] OpticalFlowTracker(opencv_core.IplImage flow, LinkedList<opencv_core.CvPoint2D32f> points_in) {

        LinkedList<opencv_core.CvPoint2D32f> points_out = new LinkedList<>();
        int[] status = new int[points_in.size()];

        int width = flow.width();
        int height = flow.height();
        for (int i = 0; i < points_in.size(); i++) {

            opencv_core.CvPoint2D32f point_in = points_in.get(i);
            LinkedList<Float> xs = new LinkedList<>();
            LinkedList<Float> ys = new LinkedList<>();
            int x = cvFloor(point_in.x());
            int y = cvFloor(point_in.y());
            for (int m = x - 1; m <= x + 1; m++) {
                for (int n = y - 1; n <= y + 1; n++) {
                    int p = Math.min(Math.max(m, 0), width - 1);
                    int q = Math.min(Math.max(n, 0), height - 1);

                    FloatBuffer floatBuffer = flow.getByteBuffer(q * flow.widthStep()).asFloatBuffer();
                    int xsIndex = 2 * p;
                    int ysIndex = 2 * p + 1;

                    xs.addLast(floatBuffer.get(xsIndex));
                    ys.addLast(floatBuffer.get(ysIndex));
//                    if (i < 0){
//                        System.out.println("i: " + i
//                                + ", x: " + x + ", y: " + y
//                                + ", m: " + m + ", n: " + n
//                                + ", p: " + p + ", q: " + q
//                                + ", xs: " + floatBuffer.get(xsIndex) + ", ys: " + floatBuffer.get(ysIndex)
//                        );
//                    }
                }
            }
            xs.sort(Float::compare);
            ys.sort(Float::compare);
            int size = xs.size() / 2;
            for (int m = 0; m < size; m++) {
                xs.removeLast();
                ys.removeLast();
            }

            opencv_core.CvPoint2D32f offset = new opencv_core.CvPoint2D32f();
            offset.x(xs.getLast());
            offset.y(ys.getLast());

            opencv_core.CvPoint2D32f point_out = new opencv_core.CvPoint2D32f();
            point_out.x(point_in.x() + offset.x());
            point_out.y(point_in.y() + offset.y());

            //TODO: note we used a different approach than the c++ version
            points_out.addLast(point_out);
            if (point_out.x() > 0 && point_out.x() < width && point_out.y() > 0 && point_out.y() < height) {
                status[i] = 1;
//                if ((i > 137 && i < 144) || i < 6){
//                    System.out.println("s(1)_i: " + i + ", pin: " + point_in + ", pout: " + point_out + ", off: " + offset);
//                }
            } else {
                status[i] = -1;
//                System.out.println("s(-1)_i: " + i + ", w: " + width + ", h: " + height
//                        + ", pin: " + point_in + ", pout: " + point_out + ", off: " + offset);
            }
        }
        return new Object[]{points_out, status};
    }

    //We have re-organized the input and output to the oringal c++ version
    public static DescMat[] MbhComp(IplImage flow, DescInfo descInfo, int width, int height) {
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

    public static DescMat HogComp(IplImage img, DescInfo descInfo, int width, int height) {
        IplImage imgX = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);
        IplImage imgY = cvCreateImage(cvSize(width, height), IPL_DEPTH_32F, 1);

        opencv_imgproc.cvSobel(img, imgX, 1, 0, 1);
        opencv_imgproc.cvSobel(img, imgY, 0, 1, 1);
        DescMat retVal = BuildDescMat(imgX, imgY, descInfo, width, height);
        cvReleaseImage(imgX);
        cvReleaseImage(imgY);

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

    public static CvScalar getRect(CvPoint2D32f point, CvSize size, DescInfo descInfo) {
        int x_min = descInfo.blockWidth / 2;
        int y_min = descInfo.blockHeight / 2;
        int x_max = size.width() - descInfo.blockWidth;
        int y_max = size.height() - descInfo.blockHeight;

        float tmp_x = point.x() - x_min;
        float temp_x = Math.min(Math.max(tmp_x, 0.0f), (float) x_max);

        float tmp_y = point.y() - y_min;
        float temp_y = Math.min(Math.max(tmp_y, 0.0f), (float) y_max);

        CvScalar rect = new CvScalar();
        rect.setVal(0, temp_x);
        rect.setVal(1, temp_y);
        rect.setVal(2, descInfo.blockWidth);
        rect.setVal(3, descInfo.blockHeight);

        return rect;
    }

    public static CvScalar getRect(Serializable.CvPoint2D32f point, CvSize size, DescInfo descInfo) {
        int x_min = descInfo.blockWidth / 2;
        int y_min = descInfo.blockHeight / 2;
        int x_max = size.width() - descInfo.blockWidth;
        int y_max = size.height() - descInfo.blockHeight;

        float tmp_x = point.x() - x_min;
        float temp_x = Math.min(Math.max(tmp_x, 0.0f), (float) x_max);

        float tmp_y = point.y() - y_min;
        float temp_y = Math.min(Math.max(tmp_y, 0.0f), (float) y_max);

        CvScalar rect = new CvScalar();
        rect.setVal(0, temp_x);
        rect.setVal(1, temp_y);
        rect.setVal(2, descInfo.blockWidth);
        rect.setVal(3, descInfo.blockHeight);

        return rect;
    }

    public static float[] getDesc(DescMat descMat, CvScalar rect, DescInfo descInfo) {
        int descDim = descInfo.dim;
        int height = descMat.height;
        int width = descMat.width;

        float[] vec = new float[descDim];
        int xOffset = (int) rect.getVal(0);
        int yOffset = (int) rect.getVal(1);
        int xStride = (int) (rect.getVal(2)/descInfo.ntCells);
        int yStride = (int) (rect.getVal(3)/descInfo.ntCells);

        int iDesc = 0;
        for (int iX = 0; iX < descInfo.nxCells; iX++) {
            for (int iY = 0; iY < descInfo.nyCells; iY++) {
                int left = xOffset + iX * xStride - 1;
                int right = Math.min(left + xStride, width - 1);
                int top = yOffset + iY * yStride - 1;
                int bottom = Math.min(top + yStride, height - 1);

                int topLeft = (top * width + left) * descInfo.nBins;
                int topRight = (top * width + right) * descInfo.nBins;
                int bottomLeft = (bottom * width + left) * descInfo.nBins;
                int bottomRight = (bottom * width + right) * descInfo.nBins;

                for (int i = 0; i < descInfo.nBins; i++, iDesc++) {
                    double sumTopLeft = 0.0;
                    double sumTopRight = 0.0;
                    double sumBottomLeft = 0.0;
                    double sumBottomRight = 0.0;

                    if (top >= 0) {
                        if (left >= 0) {
                            sumTopLeft = descMat.desc[topLeft + i];
                        }
                        if (right >= 0) {
                            sumTopRight = descMat.desc[topRight + i];
                        }
                    }
                    if (bottom >= 0) {
                        if (left >= 0) {
                            sumBottomLeft = descMat.desc[bottomLeft + i];
                        }
                        if (right >= 0) {
                            sumBottomRight = descMat.desc[bottomRight + i];
                        }
                    }
                    float temp = (float) (sumBottomRight + sumTopLeft - sumBottomLeft - sumTopRight);
                    vec[iDesc] = Math.max(temp, 0.0f) + 0.05f;
                }
            }
        }

        if (descInfo.norm == 1) {
            return norm_1(vec);
        } else {
            return norm_2(vec);
        }
    }

    public static float[] norm_1(float[] input) {
        float[] retVal = new float[input.length];

        double sum = 0.0;
        for (int i = 0; i < input.length; i++) {
            sum += input[i];
        }
        sum += 0.0000000001;

        for (int i = 0; i < input.length; i++) {
            retVal[i] = (float) (input[i] / sum);
        }
        return retVal;
    }

    public static float[] norm_2(float[] input) {
        float[] retVal = new float[input.length];

        double sum = 0.0;
        for (int i = 0; i < input.length; i++) {
            sum += (input[i] * input[i]);
        }
        sum += 0.0000000001;
        sum = Math.sqrt(sum);

        for (int i = 0; i < input.length; i++) {
            retVal[i] = (float) (input[i] / sum);
        }
        return retVal;
    }

    public static Object[] isValid(CvPoint2D32f[] track) {
        float min_var = 1.732f;
        float max_var = 50;
        float max_dis = 20;

        float mean_x = 0;
        float mean_y = 0;
        float var_x = 0;
        float var_y = 0;
        float length = 0;

        int size = track.length;
        CvPoint2D32f[] bk = new CvPoint2D32f[size];
        for (int i = 0; i < size; i++) {
            bk[i] = new CvPoint2D32f();
            //mean_x += track[i].x();
            //mean_y += track[i].y();
            bk[i].x(track[i].x());
            bk[i].y(track[i].y());
            mean_x += bk[i].x();
            mean_y += bk[i].y();
        }

        mean_x /= size;
        mean_y /= size;

        for (int i = 0; i < size; i++) {
            float tmpX = bk[i].x();
            bk[i].x(tmpX - mean_x);
            var_x += (bk[i].x() * bk[i].x());

            float tmpY = bk[i].y();
            bk[i].y(tmpY - mean_y);
            var_y += (bk[i].y() * bk[i].y());
        }

        var_x /= size;
        var_y /= size;
        var_x = (float) Math.sqrt(var_x);
        var_y = (float) Math.sqrt(var_y);

        if (var_x < min_var && var_y < min_var) {
            return new Object[]{null, null, 0};
        }

        if (var_x > max_var || var_x > max_var) {
            return new Object[]{null, null, 0};
        }

        for (int i = 1; i < size; i++) {
            float temp_x = bk[i].x() - bk[i - 1].x();
            float temp_y = bk[i].y() - bk[i - 1].y();
            length += Math.sqrt(temp_x * temp_x + temp_y * temp_y);
            bk[i - 1].x(temp_x);
            bk[i - 1].y(temp_y);
        }

        float len_thre = length * 0.7f;
        for (int i = 0; i < size - 1; i++) {
            float temp_x = bk[i].x();
            float temp_y = bk[i].y();
            float temp_dis = (float) Math.sqrt(temp_x * temp_x + temp_y * temp_y);
            if (temp_dis > max_dis && temp_dis > len_thre) {
                return new Object[]{null, null, 0};
            }
        }

        CvPoint2D32f[] bk2 = java.util.Arrays.copyOfRange(bk, 0, size - 1);
        for (int i = 0; i < size - 1; i++) {
            float tmpX = bk2[i].x() / length;
            bk2[i].x(tmpX);

            float tmpY = bk2[i].y() / length;
            bk2[i].y(tmpY);
        }

        return new Object[]{bk2, new float[]{mean_x, mean_y, var_x, var_y, length}, 1};
    }

    public static int isValid(List<Serializable.CvPoint2D32f> track) {
        float min_var = 1.732f;
        float max_var = 50;
        float max_dis = 20;

        float mean_x = 0;
        float mean_y = 0;
        float var_x = 0;
        float var_y = 0;
        float length = 0;

        int size = track.size();
        CvPoint2D32f[] bk = new CvPoint2D32f[size];
        for (int i = 0; i < size; i++) {
            bk[i] = new CvPoint2D32f();
            //mean_x += track[i].x();
            //mean_y += track[i].y();
            bk[i].x(track.get(i).x());
            bk[i].y(track.get(i).y());
            mean_x += bk[i].x();
            mean_y += bk[i].y();
        }

        mean_x /= size;
        mean_y /= size;

        for (int i = 0; i < size; i++) {
            float tmpX = bk[i].x();
            bk[i].x(tmpX - mean_x);
            var_x += (bk[i].x() * bk[i].x());

            float tmpY = bk[i].y();
            bk[i].y(tmpY - mean_y);
            var_y += (bk[i].y() * bk[i].y());
        }

        var_x /= size;
        var_y /= size;
        var_x = (float) Math.sqrt(var_x);
        var_y = (float) Math.sqrt(var_y);

        if (var_x < min_var && var_y < min_var) {
            return 0;
        }

        if (var_x > max_var || var_x > max_var) {
            return 0;
        }

        for (int i = 1; i < size; i++) {
            float temp_x = bk[i].x() - bk[i - 1].x();
            float temp_y = bk[i].y() - bk[i - 1].y();
            length += Math.sqrt(temp_x * temp_x + temp_y * temp_y);
            bk[i - 1].x(temp_x);
            bk[i - 1].y(temp_y);
        }

        float len_thre = length * 0.7f;
        for (int i = 0; i < size - 1; i++) {
            float temp_x = bk[i].x();
            float temp_y = bk[i].y();
            float temp_dis = (float) Math.sqrt(temp_x * temp_x + temp_y * temp_y);
            if (temp_dis > max_dis && temp_dis > len_thre) {
                return 0;
            }
        }

        return 1;
    }

    public static boolean isValid(Serializable.CvPoint2D32f[] track) {
        float min_var = 1.732f;
        float max_var = 50;
        float max_dis = 20;

        float mean_x = 0;
        float mean_y = 0;
        float var_x = 0;
        float var_y = 0;
        float length = 0;

        int size = track.length;
        CvPoint2D32f[] bk = new CvPoint2D32f[size];
        for (int i = 0; i < size; i++) {
            bk[i] = new CvPoint2D32f();
            bk[i].x(track[i].x());
            bk[i].y(track[i].y());
            mean_x += bk[i].x();
            mean_y += bk[i].y();
        }

        mean_x /= size;
        mean_y /= size;

        for (int i = 0; i < size; i++) {
            float tmpX = bk[i].x();
            bk[i].x(tmpX - mean_x);
            var_x += (bk[i].x() * bk[i].x());

            float tmpY = bk[i].y();
            bk[i].y(tmpY - mean_y);
            var_y += (bk[i].y() * bk[i].y());
        }

        var_x /= size;
        var_y /= size;
        var_x = (float) Math.sqrt(var_x);
        var_y = (float) Math.sqrt(var_y);

        if (var_x < min_var && var_y < min_var) {
            return false;
        }

        if (var_x > max_var || var_x > max_var) {
            return false;
        }

        for (int i = 1; i < size; i++) {
            float temp_x = bk[i].x() - bk[i - 1].x();
            float temp_y = bk[i].y() - bk[i - 1].y();
            length += Math.sqrt(temp_x * temp_x + temp_y * temp_y);
            bk[i - 1].x(temp_x);
            bk[i - 1].y(temp_y);
        }

        float len_thre = length * 0.7f;
        for (int i = 0; i < size - 1; i++) {
            float temp_x = bk[i].x();
            float temp_y = bk[i].y();
            float temp_dis = (float) Math.sqrt(temp_x * temp_x + temp_y * temp_y);
            if (temp_dis > max_dis && temp_dis > len_thre) {
                return false;
            }
        }

        return true;
    }

    public static void WriteTrajFeature2Txt(
            BufferedWriter oStream, float[] Track_Info, float[] XYs, List<Float> MBHfeature, float[] TRJfeature) throws IOException {
        for (int i = 0; i < Track_Info.length; i++) {
            oStream.write(Track_Info[i] + " ");
        }
        for (int i = 0; i < XYs.length; i++) {
            oStream.write(XYs[i] + " ");
        }
        for (int i = 0; i < MBHfeature.size(); i++) {
            oStream.write(MBHfeature.get(i) + " ");
        }
        for (int i = 0; i < TRJfeature.length; i++) {
            oStream.write(TRJfeature[i] + " ");
        }
        oStream.newLine();
    }

    public static byte[] toBytes(List<float[]> data) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        try {
            dout.writeInt(data.size());
        } catch (IOException e) {
            // never arrive here
        }
        data.stream().forEach(arr -> {
            try {
                dout.writeInt(arr.length);
                for (int i = 0; i < arr.length; i++) {
                    dout.writeFloat(arr[i]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return bout.toByteArray();
    }

    public static List<float[]> readArrays(byte[] in) {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(in));
        List<float[]> data = null;
        try {
            int size = input.readInt();
            data = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                float[] arr = new float[input.readInt()];
                for (int j = 0; j < arr.length; j++) {
                    arr[j] = input.readFloat();
                }
                data.add(arr);
            }
        } catch (IOException e) {
            // never arrive here
        }
        return data;
    }
}

package test;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_features2d;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import org.junit.Test;
import tool.Serializable;

import java.io.IOException;

/**
 * Created by nurlan on 8/21/14.
 */
public class InputStreamTest {

    private String SOURCE_FILE;
    private int frameId;
    private long lastFrameTime;

    int firstFrameId ;
    int lastFrameId ;
    FFmpegFrameGrabber grabber;

    public void prepare() throws IOException, FrameGrabber.Exception {
        opencv_features2d.KeyPoint kp = new opencv_features2d.KeyPoint();
        //SOURCE_FILE = "/Users/nurlan/Desktop/1.mp4";
        SOURCE_FILE = "/home/storm/logo-dect/video/1.mp4";
        grabber = new FFmpegFrameGrabber(SOURCE_FILE);
        grabber.start();
        frameId = 0;
        firstFrameId = 0;
        lastFrameId = 100;

        while (++frameId < firstFrameId)
            grabber.grab();
        kp.deallocate();
    }

    opencv_core.IplImage iplImage;
    opencv_core.Mat matImage;

    public void nextTuple() throws IOException, FrameGrabber.Exception {
        long now = System.currentTimeMillis();
        if (now - lastFrameTime < 100){
            return;
        }else {
            lastFrameTime=now;
        }

        if (frameId < lastFrameId) {
            iplImage = grabber.grab();
            matImage = new opencv_core.Mat(iplImage);
            Serializable.Mat sMat = new Serializable.Mat(matImage);
            System.out.println(sMat.getCols() + "x" + sMat.getRows() + " of " + sMat.getType());
            frameId ++;
        }
    }

    @Test
    public void runTest() throws IOException, FrameGrabber.Exception {
        prepare();
        while (frameId < lastFrameId) {
            nextTuple();
        }
    }

}

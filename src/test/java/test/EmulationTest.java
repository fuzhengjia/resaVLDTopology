package test;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_features2d;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.OpenCVFrameGrabber;
import org.junit.Test;
import tool.Serializable;

/**
 * Created by nurlan on 8/20/14.
 */
public class EmulationTest {
    private String SOURCE_FILE;
    //private FrameGrabber grabber;
    private FrameGrabber grabber;
    private int frameId;
    private long lastFrameTime;

    int firstFrameId ;
    int lastFrameId ;
    opencv_core.Mat mat;

    public void prepare() throws FrameGrabber.Exception {
        SOURCE_FILE = "/Users/nurlan/Desktop/1.mp4";
        grabber = new OpenCVFrameGrabber(SOURCE_FILE);
        opencv_features2d.KeyPoint kp = new opencv_features2d.KeyPoint();
        frameId = 0;
        firstFrameId = 0;
        lastFrameId = 1000;

        mat = new opencv_core.Mat();

        kp.deallocate();
        grabber.start();

        while (++frameId < firstFrameId)
            grabber.grab();

    }



    public void nextTuple() throws FrameGrabber.Exception {
        long now = System.currentTimeMillis();
        if (now - lastFrameTime < 100){
            return;
        }else {
            lastFrameTime=now;
        }
        opencv_core.IplImage image;

        if (frameId < lastFrameId) {
            image = grabber.grab();
            Serializable.Mat sMat = new Serializable.Mat(new opencv_core.Mat(image));
            System.out.println(sMat.getCols() + "x" + sMat.getRows() + " of " + sMat.getType());
            frameId ++;
        }
    }

    @Test
    public void runTest() throws FrameGrabber.Exception {
        prepare();
        while (frameId < lastFrameId) {
            nextTuple();
        }
    }


}

package test;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by nurlan on 8/20/14.
 */
public class GetByteBufferTest {
    @Test
    public void runTest() throws IOException {
        opencv_highgui.VideoCapture capture = new opencv_highgui.VideoCapture(0);
        //String file = "/home/storm/logo-dect/logo/mc2.jpg";
        String file = "/Users/nurlan/Desktop/mc2.jpg";
        opencv_core.Mat mat = new opencv_core.Mat(opencv_core.IplImage.createFrom(ImageIO.read(new FileInputStream(file))));
        ByteBuffer byteBuffer = mat.getByteBuffer();
        byteBuffer.rewind();
        System.out.println(byteBuffer.capacity());
        capture.release();
    }
}

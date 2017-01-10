package tool;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;

import java.io.File;


/**
 * Created by tomFu on Aug 3, special design for fox version!!!
 * Shall use FrameImplImageSourceFox
 * In summary, from imageSender (IplImage->Mat->sMat->byte[]) to redis queue -> byte[]->sMat->Mat->IplImage)
 * Need test!!!
 * When call this function, the machine with physical camera sensor may need use "sodu" to get root access.
 * through testing
 */
public class testCamera {
    public static void main(String[] args) throws Exception {

        opencv_core.IplImage fk = new opencv_core.IplImage();
        //opencv_highgui.VideoCapture camera = new opencv_highgui.VideoCapture();
        File f = new File("C:\\Users\\Tom.fu\\Dropbox\\Resa-project\\Demo-videos\\vld-camera-input.mp4");
        System.out.println("file exist: " + f.exists());

        FFmpegFrameGrabber streamGrabber = new FFmpegFrameGrabber("C:\\Users\\Tom.fu\\Dropbox\\Resa-project\\Demo-videos\\vld-camera-input.mp4");

        //streamGrabber = new FFmpegFrameGrabber("rtsp://admin:12345@192.168.64.96:554/live1");
        streamGrabber.setFrameRate(25);
        //streamGrabber.setImageWidth(getWidth());
        String windowName = "test-camera";
        opencv_highgui.namedWindow(windowName, opencv_highgui.WINDOW_AUTOSIZE);

        try {
            streamGrabber.start();

            while(true)
            {
                opencv_core.IplImage image = streamGrabber.grab();
                opencv_core.Mat mat = new opencv_core.Mat(image);
                //camera.read(mat);

                opencv_highgui.imshow(windowName, mat);
                opencv_highgui.waitKey(1);

            }

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }

        //camera.open("C:\\Users\\Tom.fu\\Dropbox\\Resa-project\\Demo-videos\\vld-camera-input.mp4");
        //camera.open("http://183.3.139.134:9001/status.html");
//        Thread.sleep(1000);
//        if (!camera.isOpened()) {
//            System.out.println("Camera Error");
//        } else {
//            System.out.println("Camera OK?");
//
//
//
//            while(true)
//            {
//                opencv_core.Mat mat = new opencv_core.Mat();
//                camera.read(mat);
//
//                opencv_highgui.imshow(windowName, mat);
//                opencv_highgui.waitKey(1);
//
//            }

//            Jedis jedis = new Jedis("localhost", 6379);
//            byte[] queueName = "testSource".getBytes();
//            int generatedFrames = 0;
//            long qLen = 0;
//
//            while (true) {
//
//                opencv_core.Mat mat = new opencv_core.Mat();
//                camera.read(mat);
//
//                Serializable.Mat sMat = new Serializable.Mat(mat);
//                jedis.rpush(queueName, sMat.toByteArray());
//
//                Thread.sleep(1000);
//                qLen = jedis.llen(queueName);
//                System.out.println("totalSend: " + generatedFrames + ", sendQLen: " + qLen + ", W: " + mat.cols() + ", H: " + mat.rows());
//            }



    }
}

package tool;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import redis.clients.jedis.Jedis;

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
        opencv_highgui.VideoCapture camera = new opencv_highgui.VideoCapture();
        camera.open("rtsp://admin:admin@192.168.0.203:554/cam/realmonitor?channel=1&subtype=0");
        //camera.open("rtsp://admin:admin@192.168.0.203:554/cam/realmonitor?channel=1&subtype=0");
        Thread.sleep(1000);
        if (!camera.isOpened()) {
            System.out.println("Camera Error");
        } else {
            System.out.println("Camera OK?");

            Jedis jedis = new Jedis("localhost", 6379);
            byte[] queueName = "testSource".getBytes();
            int generatedFrames = 0;
            long qLen = 0;

            while (true) {

                opencv_core.Mat mat = new opencv_core.Mat();
                camera.read(mat);

                Serializable.Mat sMat = new Serializable.Mat(mat);
                jedis.rpush(queueName, sMat.toByteArray());

                Thread.sleep(1000);
                qLen = jedis.llen(queueName);
                System.out.println("totalSend: " + generatedFrames + ", sendQLen: " + qLen + ", W: " + mat.cols() + ", H: " + mat.rows());
            }
        }


    }
}

package tool;

import backtype.storm.Config;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import static topology.StormConfigManager.*;

/**
 * Created by ding on 14-3-18.
 */
public class SimpleCameraSender {

    private String host;
    private int port;
    private byte[] queueName;
    //private String sourceVideoFile;

    //private FFmpegFrameGrabber grabber;
    private opencv_highgui.VideoCapture camera;

    public SimpleCameraSender(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
    }

    public SimpleCameraSender(String confile, String qName) throws FileNotFoundException {
        this(confile);
        this.queueName = qName.getBytes();
    }

    public void send2Queue(int fps, int startID, int targetCount) throws IOException {
        Jedis jedis = new Jedis(host, port);
        int generatedFrames = 0;
        opencv_core.IplImage fk = new opencv_core.IplImage();

        try {
            camera = new opencv_highgui.VideoCapture(0);
            Thread.sleep(1000);
            if (!camera.isOpened()) {
                System.out.println("Camera Error");
            } else {
                System.out.println("Camera OK?");
            }

            long start = System.currentTimeMillis();
            long last = start;
            long qLen = 0;

            while (generatedFrames < targetCount) {
                opencv_core.Mat mat = new opencv_core.Mat();
                camera.read(mat);
                BufferedImage bufferedImage = mat.getBufferedImage();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(bufferedImage, "JPEG", baos);
                jedis.rpush(this.queueName, baos.toByteArray());
                generatedFrames ++;
                if (generatedFrames % fps == 0) {
                    long current = System.currentTimeMillis();
                    long elapse = current - last;
                    long remain = 1000 - elapse;
                    if (remain > 0) {
                        Thread.sleep(remain);
                    }
                    last = System.currentTimeMillis();
                    qLen = jedis.llen(this.queueName);
                    System.out.println("Current: " + last + ", elapsed: " + (last - start)
                            + ",totalSend: " + generatedFrames+ ", remain: " + remain + ", sendQLen: " + qLen);
                } else {
//                    long current = System.currentTimeMillis();
//                    long elapse = current - last;
//                    long remain = 1000 - elapse;
//                    int remainCnt = generatedFrames % fps;
//                    long wait = remain / remainCnt;
//                    if (wait > 0) {
//                        Thread.sleep(wait);
//                    }
                }
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        camera.release();
        System.out.println("Exit send2Queue, totalSend: " + generatedFrames);
    }

    public static void main(String[] args) throws Exception {
        SimpleCameraSender sender;
        int fps;
        int startFrameID;
        int targetCount;
        if (args.length < 4 || args.length > 5) {
            System.out.println("usage: ImageSender <confFile> [queueName] <fps> <startFrame> <targetCount>");
            return;
        }

        if (args.length == 4) {
            sender = new SimpleCameraSender(args[0]);
            System.out.println("Default queueName: " + sender.queueName.toString());
            startFrameID = Integer.parseInt(args[1]);
            targetCount = Integer.parseInt(args[2]);
            fps = Integer.parseInt(args[3]);

        } else {
            sender = new SimpleCameraSender(args[0], args[1]);
            System.out.println("User-defined queueName: " + args[1]);
            startFrameID = Integer.parseInt(args[2]);
            targetCount = Integer.parseInt(args[3]);
            fps = Integer.parseInt(args[4]);
        }
        System.out.println("start sender, queueName: "
                + sender.queueName.toString() + ", fps: " + fps + ", start: " + startFrameID + ", target: " + targetCount);
        sender.send2Queue(fps, startFrameID, targetCount);
        System.out.println("end sender");
    }

}

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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static topology.StormConfigManager.*;

/**
 * Created by ding on 14-3-18.
 */
public class SimpleVideoSender {

    private String host;
    private int port;
    private byte[] queueName;
    private String sourceVideoFile;

    private FFmpegFrameGrabber grabber;

    public SimpleVideoSender(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
        this.sourceVideoFile = getString(conf, "sourceVideoFile");
    }

    public SimpleVideoSender(String confile, String qName) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = qName.getBytes();
        this.sourceVideoFile = getString(conf, "sourceVideoFile");
    }

    public void send2Queue(int fps, int targetCount) throws IOException {
        Jedis jedis = new Jedis(host, port);

        grabber = new FFmpegFrameGrabber(sourceVideoFile);
        int generatedFrames = 0;
        long start = System.currentTimeMillis();
        //long waitDuration = (long)(1000.0 / (double)fps);
        long last = start;
        try {
            grabber.start();
            while (generatedFrames < targetCount) {
                opencv_core.IplImage image = grabber.grab();
                opencv_core.Mat mat = new opencv_core.Mat(image);
                BufferedImage bufferedImage = mat.getBufferedImage();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(bufferedImage, "JPEG", baos);
                jedis.rpush(this.queueName, baos.toByteArray());
                long current = System.currentTimeMillis();
                System.out.println("Current: " + current + ", elapsed: " + (current - start) + ",totalSend: " + generatedFrames);
                generatedFrames ++;
                if (generatedFrames % fps == 0) {
                    long remainTime = current - last;
                    if (remainTime < 990) {
                        System.out.println("generatedFrames: " + generatedFrames + "remain: " + remainTime);
                        Thread.sleep(remainTime);
                    }
                    last = System.currentTimeMillis();
                }
            }
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println("Exit send2Queue, totalSend: " + generatedFrames);
    }

    public static void main(String[] args) throws Exception {
        SimpleVideoSender sender;
        int fps;
        int targetCount;
        if (args.length < 3 || args.length > 4) {
            System.out.println("usage: ImageSender <confFile> [queueName] <fps> <targetCount>");
            return;
        }

        if (args.length == 3) {
            sender = new SimpleVideoSender(args[0]);
            System.out.println("Default queueName: " + sender.queueName.toString());
            fps = Integer.parseInt(args[1]);
            targetCount = Integer.parseInt(args[2]);
        } else {
            sender = new SimpleVideoSender(args[0], args[1]);
            System.out.println("User-defined queueName: " + args[1]);
            fps = Integer.parseInt(args[2]);
            targetCount = Integer.parseInt(args[3]);
        }
        System.out.println("start sender, queueName: " + sender.queueName.toString() + ", fps: " + fps + ", target: " + targetCount);
        sender.send2Queue(fps, targetCount);
        System.out.println("end sender");
    }

}

package tool;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import backtype.storm.Config;
import util.ConfigUtil;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;
import static topology.StormConfigManager.readConfig;

/**
 * Created by ding on 14-3-18.
 */
public class SimpleImageSender {

    private String host;
    private int port;
    private byte[] queueName;
    private String path;
    private String imageFolder;
    private String filePrefix;
    //private BlockingQueue<File> dataQueue = new ArrayBlockingQueue<>(10000);

    public SimpleImageSender(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");
        this.filePrefix = getString(conf, "filePrefix");
    }

    public SimpleImageSender(String confile, String qName) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = qName.getBytes();
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");
        this.filePrefix = getString(conf, "filePrefix", "frame");
    }

    public void send2Queue(int st, int end, int fps) throws IOException {
        Jedis jedis = new Jedis(host, port);
        int generatedFrames = st;
        int targetCount = end - st;

        try {
            long start = System.currentTimeMillis();
            long last = start;
            long qLen = 0;

            while (generatedFrames < targetCount) {

                String fileName = path + imageFolder + System.getProperty("file.separator")
                        + String.format("%s%06d.jpg", filePrefix, (generatedFrames + 1));
                File f = new File(fileName);
                if (f.exists() == false) {
                    System.out.println("File not exist: " + fileName);
                    continue;
                }
                //System.out.println(fileName);
                opencv_core.IplImage imageFk = cvLoadImage(fileName);
                opencv_core.Mat matOrg = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);
                BufferedImage bufferedImage = matOrg.getBufferedImage();
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
                }
            }

        } catch (InterruptedException e){
            e.printStackTrace();
        }

        /*
        for (int i = st; i < end; i ++) {
            String fileName = path + "Seq01_color" + System.getProperty("file.separator")
                                    + String.format("frame%06d.jpg", (i + 1));
            File f = new File(fileName);
            if (f.exists() == false) {
                System.out.println("File not exist: " + fileName);
                continue;
            }
            System.out.println(fileName);

            opencv_core.Mat matOrg = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);
            BufferedImage bufferedImage = matOrg.getBufferedImage();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, "JPEG", baos);
            jedis.rpush(this.queueName, baos.toByteArray());
        }
        */
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("usage: ImageSender <confFile> queueName <st> <end> <fps>");
            return;
        }
        SimpleImageSender sender = new SimpleImageSender(args[0], args[1]);
        System.out.println("start sender");
        sender.send2Queue(Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        System.out.println("end sender");
    }

}

package tool;

import backtype.storm.Config;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import redis.clients.jedis.Jedis;
import topology.Serializable;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.ByteBuffer;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static topology.StormConfigManager.*;

/**
 * Created by ding on 14-3-18.
 */
public class Img2Mat2BArrSender {

    private String host;
    private int port;
    private byte[] queueName;
    private String path;
    private String imageFolder;

    //private BlockingQueue<File> dataQueue = new ArrayBlockingQueue<>(10000);

    public Img2Mat2BArrSender(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
        this.path = getString(conf, "sourceFilePath");
    }

    public Img2Mat2BArrSender(String confile, String qName) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = qName.getBytes();
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");
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
                        + String.format("frame%06d.jpg", (generatedFrames + 1));
                File f = new File(fileName);
                if (f.exists() == false) {
                    System.out.println("File not exist: " + fileName);
                    continue;
                }
                //System.out.println(fileName);
                opencv_core.IplImage imageFk = cvLoadImage(fileName);
                opencv_core.Mat matOrg = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);
                Serializable.Mat sMatOrg = new Serializable.Mat(matOrg);

                BufferedImage bufferedImage = matOrg.getBufferedImage();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                Output output = new Output(baos);
                Kryo kryo = new Kryo();
                sMatOrg.write(kryo, output);
                //ImageIO.write(bufferedImage, "JPEG", baos);
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
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("usage: ImageSender <confFile> queueName <st> <end> <fps>");
            return;
        }
        Img2Mat2BArrSender sender = new Img2Mat2BArrSender(args[0], args[1]);
        System.out.println("start sender");
        sender.send2Queue(Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        System.out.println("end sender");
    }

}

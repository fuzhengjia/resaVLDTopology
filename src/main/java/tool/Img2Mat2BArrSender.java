package tool;

import backtype.storm.Config;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import redis.clients.jedis.Jedis;
import util.ConfigUtil;

import java.io.*;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
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

    private int nChannel;
    private int nDepth;
    private int inHeight;
    private int inWidth;

    //private BlockingQueue<File> dataQueue = new ArrayBlockingQueue<>(10000);

    public Img2Mat2BArrSender(String confile) throws FileNotFoundException {

        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");

        nChannel = ConfigUtil.getInt(conf, "nChannel", 3);
        nDepth = ConfigUtil.getInt(conf, "nDepth", 8);
        inWidth = ConfigUtil.getInt(conf, "inWidth", 640);
        inHeight = ConfigUtil.getInt(conf, "inHeight", 480);
    }

    public Img2Mat2BArrSender(String confile, String qName) throws FileNotFoundException {
        this(confile);
        this.queueName = qName.getBytes();
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
                opencv_core.IplImage image = cvLoadImage(fileName);
                //opencv_core.Mat matOrg = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);
                opencv_core.IplImage frame = cvCreateImage(cvSize(inWidth, inHeight), nDepth, nChannel);
                opencv_imgproc.cvResize(image, frame, opencv_imgproc.CV_INTER_AREA);

                //opencv_core.Mat matNew = new opencv_core.Mat();
                //opencv_imgproc.resize(matOrg, matNew, newSize);

                //Serializable.Mat sMat = new Serializable.Mat(matNew);
                opencv_core.Mat matNew = new opencv_core.Mat(frame);
                Serializable.Mat sMat = new Serializable.Mat(matNew);

                byte[] data = sMat.toByteArray();
                jedis.rpush(this.queueName, data);

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

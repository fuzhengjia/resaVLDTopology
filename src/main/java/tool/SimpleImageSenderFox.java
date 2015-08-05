package tool;

import backtype.storm.Config;
import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static topology.StormConfigManager.*;

/**
 * Created by tomFu on Aug 3, special design for actdet_fox version!!!
 * Shall use FrameImplImageSourceFox
 *  In summary, from imageSender (IplImage->Mat->sMat->byte[]) to redis queue -> byte[]->sMat->Mat->IplImage)
 */
public class SimpleImageSenderFox {

    private String host;
    private int port;
    private byte[] queueName;
    private String path;
    private String imageFolder;
    private String filePrefix;

    public SimpleImageSenderFox(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");
        this.filePrefix = getString(conf, "filePrefix", "frame");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
    }

    public SimpleImageSenderFox(String confile, String qName) throws FileNotFoundException {
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
                        + String.format("%s%06d.jpg", filePrefix, (generatedFrames + 1));
                File f = new File(fileName);
                if (f.exists() == false) {
                    System.out.println("File not exist: " + fileName);
                    continue;
                }

                opencv_core.IplImage image = cvLoadImage(fileName);
                opencv_core.Mat matOrg = new opencv_core.Mat(image);
                Serializable.Mat sMat = new Serializable.Mat(matOrg);
                jedis.rpush(this.queueName, sMat.toByteArray());

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
        SimpleImageSenderFox sender = new SimpleImageSenderFox(args[0], args[1]);
        System.out.println("start sender");
        sender.send2Queue(Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        System.out.println("end sender");
    }

}

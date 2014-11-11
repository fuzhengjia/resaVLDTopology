package tool;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
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
    //private BlockingQueue<File> dataQueue = new ArrayBlockingQueue<>(10000);

    public SimpleImageSender(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
        this.path = getString(conf, "sourceFilePath");
    }

    public void send2Queue(int st, int end) throws IOException {
        Jedis jedis = new Jedis(host, port);

        for (int i = st; i < end; i ++) {
            String fileName = path + "testdata" + System.getProperty("file.separator")
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
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: ImageSender <confFile> <st> <end>");
            return;
        }
        SimpleImageSender sender = new SimpleImageSender(args[0]);
        System.out.println("start sender");
        sender.send2Queue(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        System.out.println("end sender");
    }

}

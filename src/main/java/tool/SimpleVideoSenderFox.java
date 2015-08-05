package tool;

import backtype.storm.Config;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.io.IOException;

import static topology.StormConfigManager.*;

/**
 * Created by tomFu on Aug 3, special design for fox version!!!
 * FrameImplImageSourceFox
 *  In summary, from imageSender (IplImage->Mat->sMat->byte[]) to redis queue -> byte[]->sMat->Mat->IplImage)
 *  Need test!
 */
public class SimpleVideoSenderFox {

    private String host;
    private int port;
    private byte[] queueName;
    private String sourceVideoFile;

    private FFmpegFrameGrabber grabber;

    public SimpleVideoSenderFox(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.queueName = getString(conf, "redis.sourceQueueName").getBytes();
        this.sourceVideoFile = getString(conf, "sourceVideoFile");
    }

    public SimpleVideoSenderFox(String confile, String qName) throws FileNotFoundException {
        this(confile);
        this.queueName = qName.getBytes();
    }

    public void send2Queue(int fps, int startID, int targetCount) throws IOException {
        Jedis jedis = new Jedis(host, port);

        grabber = new FFmpegFrameGrabber(sourceVideoFile);
        int generatedFrames = 0;

        try {
            grabber.start();
            while (generatedFrames < startID){
                grabber.grab();
                generatedFrames++;
            }
            generatedFrames = 0;
            long start = System.currentTimeMillis();
            long last = start;
            long qLen = 0;

            while (generatedFrames < targetCount) {
                opencv_core.IplImage image = grabber.grab();

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
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println("Exit send2Queue, totalSend: " + generatedFrames);
    }

    public static void main(String[] args) throws Exception {
        SimpleVideoSenderFox sender;
        int fps;
        int startFrameID;
        int targetCount;
        if (args.length < 4 || args.length > 5) {
            System.out.println("usage: ImageSender <confFile> [queueName] <fps> <startFrame> <targetCount>");
            return;
        }

        if (args.length == 4) {
            sender = new SimpleVideoSenderFox(args[0]);
            System.out.println("Default queueName: " + sender.queueName.toString());
            fps = Integer.parseInt(args[1]);
            startFrameID = Integer.parseInt(args[2]);
            targetCount = Integer.parseInt(args[3]);
        } else {
            sender = new SimpleVideoSenderFox(args[0], args[1]);
            System.out.println("User-defined queueName: " + args[1]);
            fps = Integer.parseInt(args[2]);
            startFrameID = Integer.parseInt(args[3]);
            targetCount = Integer.parseInt(args[4]);
        }
        System.out.println("start sender, queueName: "
                + sender.queueName.toString() + ", fps: " + fps + ", start: " + startFrameID + ", target: " + targetCount);
        sender.send2Queue(fps, startFrameID, targetCount);
        System.out.println("end sender");
    }

}

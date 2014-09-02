package server;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Intern04 on 25/8/2014.
 */
public class TomVideoStreamSender {
    //final static String fileName = "C:\\Users\\Tom.fu\\1.mp4";
    static String fileName = null;
    FFmpegFrameGrabber grabber;

    private static final byte[] END = new String("END").getBytes();
    private String host;
    private int port;
    private byte[] queueName;
    private BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(10000);

    public TomVideoStreamSender(String host, int port, String queueName) {
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
    }

    public void send2Queue(int fps, int total) throws IOException, FrameGrabber.Exception, InterruptedException {

        grabber = new FFmpegFrameGrabber(fileName);
        grabber.start();
        new PushThread().start();

        try {
            long now;
            Set<Integer> retainFrames = new HashSet<>();
            int totalSent = 0;
            while (true) {
                now = System.currentTimeMillis();
                System.out.println("@" + now + ", qLen=" + dataQueue.size());
                for (int j = 0; j < fps; j++) {
                    opencv_core.IplImage iplImage = grabber.grab();
                    BufferedImage bufferedImage = iplImage.getBufferedImage();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(bufferedImage, "JPEG", baos);
                    dataQueue.put(baos.toByteArray());
                    totalSent++;
                }
                long sleep = now + 1000 - System.currentTimeMillis();
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }

                if (total > 0 && total < totalSent) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            grabber.release();
        }
    }

    private class PushThread extends Thread {

        private Jedis jedis = new Jedis(host, port);

        private PushThread() {
        }

        @Override
        public void run() {
            byte[] f;
            try {
                while ((f = dataQueue.take()) != END) {
                    try {
                        jedis.rpush(queueName, f);
                    } catch (Exception e) {
                    } finally {
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                dataQueue.offer(END);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("usage: TomVideoStreamSender <videoFileName> <Redis host> <Redis port> <Redis Queue> <fps> <totalFrames>");
            return;
        }

        fileName = args[0];

        TomVideoStreamSender sender = new TomVideoStreamSender(args[1], Integer.parseInt(args[2]), args[3]);
        //TomVideoStreamSender sender = new TomVideoStreamSender("192.168.0.30", 6379, "tomQ");
        //sender.send2Queue(25, 10000);
        System.out.println("start sender for file: " + fileName);
        sender.send2Queue(Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        System.out.println("end sender");
    }


}

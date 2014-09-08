package server;

import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by Intern04 on 25/8/2014.
 */
public class TomVideoStreamReceiverForLinux {

    private String host;
    private int port;
    private byte[] queueName;
    private Jedis jedis = null;

    public TomVideoStreamReceiverForLinux(String host, int port, String queueName) {
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
    }

    public void VideoStreamReceiver() throws IOException, FrameGrabber.Exception, InterruptedException {

        // Caution for the command line usage!!! pipeline
        //
        // storm jar target/resa-vld-1.0-SNAPSHOT-jar-with-dependencies.jar server.TomVideoStreamReceiverForLinux 192.168.0.30 6379 tomQ
        // | ffmpeg -f image2pipe -codec mjpeg -i pipe:0 -r 25 http://192.168.0.30:8090/feed2.ffm

        Jedis jedis = getConnectedJedis();
        byte[] baData = null;
        //OutputStream ffmpegInput = p.getOutputStream();
        int x = 0;
        long ts = System.currentTimeMillis();
        while (true) {
            try {

                baData = jedis.lpop(queueName);

                if (baData != null) {
                    BufferedImage bufferedImageRead = ImageIO.read(new ByteArrayInputStream(baData));
                    //ImageIO.write(bufferedImageRead, "JPEG", ffmpegInput);
                    ImageIO.write(bufferedImageRead, "JPEG", System.out);
                    x++;
                    //System.out.println(x);
                }
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
                disconnect();
            }
        }
    }

    // | ffmpeg -f image2pipe -codec mjpeg -i pipe:0 -r 25 http://192.168.0.30:8090/feed2.ffm
    public static void main(String args[]) {

        if (args.length < 3) {
            System.out.println("usage: TomVideoStreamReceiverForLinux <Redis host> <Redis port> <Redis Queue>");
            return;
        }
        //TomVideoStreamReceiverForLinux tvsr = new TomVideoStreamReceiverForLinux("192.168.0.30", 6379, "tomQ");
        TomVideoStreamReceiverForLinux tvsr = new TomVideoStreamReceiverForLinux(args[0], Integer.parseInt(args[1]), args[2]);
        try {
            tvsr.VideoStreamReceiver();
        } catch (Exception e) {
        }
    }

    private Jedis getConnectedJedis() {
        if (jedis != null) {
            return jedis;
        }
        //try connect to redis server
        try {
            jedis = new Jedis(host, port);
        } catch (Exception e) {
        }
        return jedis;
    }

    private void disconnect() {
        try {
            jedis.disconnect();
        } catch (Exception e) {
        }
        jedis = null;
    }


}

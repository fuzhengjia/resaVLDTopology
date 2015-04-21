package server;

import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by Intern04 on 25/8/2014.
 */
public class TomVideoStreamReceiverForWin {

    private String host;
    private int port;
    private byte[] queueName;
    private Jedis jedis = null;
    private String outputString = null;

    public TomVideoStreamReceiverForWin(String host, int port, String queueName, String outputString) {
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        this.outputString = outputString;
    }

    ///at the vlc player:, it should input: udp://@192.168.0.239:port to playback!!!
    public void VideoStreamReceiver() throws IOException, FrameGrabber.Exception, InterruptedException {

        // ffmpeg -f image2pipe -codec mjpeg -i pipe:0 -f mpegts "udp://localhost:7777"
        //ProcessBuilder pb = new ProcessBuilder(
        //        "C:\\Users\\Tom.fu\\Downloads\\ffmpeg-20140824-git-1aa153d-win64-static\\bin\\ffmpeg.exe",
        //        "-f", "image2pipe", "-codec", "mjpeg", "-i", "pipe:0", "-r", "25", "-f", "mpegts", "\"udp://localhost:7777\"");

        String ffmpegCommandString = "ffmpeg.exe";
        String pipeString = "pipe:0";

///*
        ProcessBuilder pb = new ProcessBuilder(
                "ffmpeg.exe",
                "-f", "image2pipe", "-codec", "mjpeg", "-i", "pipe:0", "-r", "25", "\"" + outputString +"\"");

        pb.redirectErrorStream(true);
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
        Process p = pb.start();

        new Thread("Webcam Process ErrorStream Consumer") {
            public void run() {
                //InputStream i = p.getInputStream();
                //BufferedInputStream bis = new BufferedInputStream(p.getInputStream());
                BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()), 4096);
                String rdLine = null;

                try {
                    //char[] buf = new char[4096];
                    while (!isInterrupted()) {
                        //i.read(buf);
                        rdLine = br.readLine();
                        if (rdLine != null) {
                            System.out.println(rdLine);
                        }
                    }
                } catch (IOException e) {
                }
            }
        }.start();
//*/
        Jedis jedis = getConnectedJedis();
        byte[] baData = null;
        OutputStream ffmpegInput = p.getOutputStream();
        int x = 0;
        long ts = System.currentTimeMillis();
        while (true) {
            try {

                baData = jedis.lpop(queueName);

                if (baData != null) {
                    BufferedImage bufferedImageRead = ImageIO.read(new ByteArrayInputStream(baData));
                    ImageIO.write(bufferedImageRead, "JPEG", ffmpegInput);
                    //ImageIO.write(bufferedImageRead, "JPEG", System.out);
                    x++;
                    System.out.println(x);
                }
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
                disconnect();
            }
        }
    }

    public static void main(String args[]) {
        if (args.length < 4) {
            System.out.println("usage: TomVideoStreamSenderForWin <Redis host> <Redis port> <Redis Queue> <output>");
            return;
        }

        //TomVideoStreamReceiverForLinux tvsr = new TomVideoStreamReceiverForLinux("192.168.0.30", 6379, "tomQ");
        TomVideoStreamReceiverForWin tvsr = new TomVideoStreamReceiverForWin(args[0], Integer.parseInt(args[1]), args[2], args[3]);

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

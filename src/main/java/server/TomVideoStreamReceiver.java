package server;

import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by Intern04 on 25/8/2014.
 */
public class TomVideoStreamReceiver {

    private String host;
    private int port;
    private byte[] queueName;
    private Jedis jedis = null;
    private boolean isWin = false;
    private String outputString = null;

    public TomVideoStreamReceiver(String host, int port, String queueName, boolean isWin, String outputString) {
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        this.isWin = isWin;
        this.outputString = outputString;
    }

    public void VideoStreamReceiver() throws IOException, FrameGrabber.Exception, InterruptedException {

        // ffmpeg -f image2pipe -codec mjpeg -i pipe:0 -f mpegts "udp://localhost:7777"
        //ProcessBuilder pb = new ProcessBuilder(
        //        "C:\\Users\\Tom.fu\\Downloads\\ffmpeg-20140824-git-1aa153d-win64-static\\bin\\ffmpeg.exe",
        //        "-f", "image2pipe", "-codec", "mjpeg", "-i", "pipe:0", "-r", "25", "-f", "mpegts", "\"udp://localhost:7777\"");

        String ffmpegCommandString = "ffmpeg.exe";
        String pipeString = "pipe:0";
        if (isWin == false){
            ffmpegCommandString = "ffmpeg";
            pipeString = "-";
        }

        ProcessBuilder pb = new ProcessBuilder(
                ffmpegCommandString,
                "-f", "image2pipe", "-codec", "mjpeg", "-i", pipeString, "-r", "25", "-threads", "0", "\"" + outputString +"\"");

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
        if (args.length < 5) {
            System.out.println("usage: TomVideoStreamSender <win or lin> <Redis host> <Redis port> <Redis Queue> <output>");
            return;
        }
        boolean isWin = false;

        if (args[0].equalsIgnoreCase("win")) {
            isWin = true;
        } else if (args[0].equalsIgnoreCase("lin")){
            isWin = false;
        } else {
            System.out.println("first argument must be win or lin!");
            return;
        }
        //TomVideoStreamReceiver tvsr = new TomVideoStreamReceiver("192.168.0.30", 6379, "tomQ");
        TomVideoStreamReceiver tvsr = new TomVideoStreamReceiver(args[1], Integer.parseInt(args[2]), args[3], isWin, args[4]);

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

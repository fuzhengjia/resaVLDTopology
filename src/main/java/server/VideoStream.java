package server;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Created by Intern04 on 25/8/2014.
 */
public class VideoStream {
    final static String fileName = "C:/Git-repos/ResaVLDTopology_org/1.mp4";

    FFmpegFrameGrabber grabber;
    int frameNumber;
    DatagramSocket socket;

    public VideoStream() throws IOException, FrameGrabber.Exception, InterruptedException {
        grabber = new FFmpegFrameGrabber(fileName);
        frameNumber = 0;
        InetAddress address = InetAddress.getByName("localhost");
        socket = new DatagramSocket(7777, address);
        grabber.start();

        // ffmpeg -f image2pipe -codec mjpeg -i pipe:0 -f mpegts "udp://localhost:7777"
        ProcessBuilder pb = new ProcessBuilder(
                "ffmpeg.exe", "-f", "image2pipe", "-codec", "mjpeg", "-re",  "-i", "pipe:0", "-f", "mpegts", "\"udp://localhost:7777\"");
        pb.redirectErrorStream(true);
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
        Process p = pb.start();
        OutputStream ffmpegInput = p.getOutputStream();
        int x = 0;
        long ts = System.currentTimeMillis();
        while (x < 100000) {
            //Thread.sleep(1000);
            opencv_core.IplImage iplImage = grabber.grab();
            BufferedImage bufferedImage = iplImage.getBufferedImage();
            ImageIO.write(bufferedImage, "JPEG", ffmpegInput);
            x ++;

            System.out.println(x);

        }

        grabber.stop();
        grabber.release();

    }
    public static void main(String args[]) {
        try {
            new VideoStream();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




}

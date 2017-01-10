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
 * Shall use FrameImplImageSourceFox
 * In summary, from imageSender (IplImage->Mat->sMat->byte[]) to redis queue -> byte[]->sMat->Mat->IplImage)
 * Need test!!!
 * When call this function, the machine with physical camera sensor may need use "sodu" to get root access.
 * through testing
 */
public class RtspCameraSenderFox {

    public String host;
    public int port;
    public byte[] queueName;
    public String videoSourceStream;
    public int fps;
    public int adjustTime;
    public int targetCount;

    private FFmpegFrameGrabber streamGrabber;

    public RtspCameraSenderFox(String confile) throws FileNotFoundException {
        Config conf = readConfig(confile);
        host = getString(conf, "rtsp.camera.out.redis.host");
        port = getInt(conf, "rtsp.camera.out.redis.port");
        queueName = getString(conf, "rtsp.camera.out.redis.queue").getBytes();
        fps = getInt(conf, "rtsp.camera.out.fps");
        adjustTime = getInt(conf, "rtsp.camera.out.adjustTime");
        targetCount = getInt(conf, "rtsp.camera.out.targetCount");
        videoSourceStream = getString(conf, "rtsp.camera.out.sourceStream");
    }

    public void send2Queue() throws IOException {
        Jedis jedis = new Jedis(host, port);
        int generatedFrames = 0;
        opencv_core.IplImage fk = new opencv_core.IplImage();
        streamGrabber = new FFmpegFrameGrabber(videoSourceStream);
        streamGrabber.setFrameRate(fps);
        try {

            streamGrabber.start();
            Thread.sleep(1000);

            long start = System.currentTimeMillis();
            long last = start;
            long qLen = 0;
            long toWait = adjustTime / fps;

            while (generatedFrames < targetCount) {
                opencv_core.IplImage image = streamGrabber.grab();
                opencv_core.Mat mat = new opencv_core.Mat(image);
                Serializable.Mat sMat = new Serializable.Mat(mat);

                jedis.rpush(this.queueName, sMat.toByteArray());

                int remainCnt = (++generatedFrames) % fps;
                if (remainCnt == 0) {
                    long current = System.currentTimeMillis();
                    long elapse = current - last;
                    long remain = 1000 - elapse;
                    if (remain > 0) {
                        Thread.sleep(remain);
                    }
                    last = System.currentTimeMillis();
                    qLen = jedis.llen(this.queueName);
                    System.out.println("Current: " + last + ", elapsed: " + (last - start)
                            + ",totalSend: " + generatedFrames+ ", remain: " + remain + ", sendQLen: " + qLen
                            + ", W: " + mat.cols() + ", H: " + mat.rows());
                }
//                else {
//                    Thread.sleep(toWait);
//                }
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        } catch (FrameGrabber.Exception e){
            e.printStackTrace();
        }

        System.out.println("Exit send2Queue, totalSend: " + generatedFrames);
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("usage: ImageSender <confFile>");
            return;
        }
        RtspCameraSenderFox sender = new RtspCameraSenderFox(args[0]);
        System.out.println("start sender, Redis queueName for video input:: "
                + sender.queueName.toString() + ", fps: " + sender.fps + ", adjust: " + sender.adjustTime
                + ", sender.target: " + sender.targetCount + ", video input String: " + sender.videoSourceStream);
        sender.send2Queue();
        System.out.println("end sender");
    }

}

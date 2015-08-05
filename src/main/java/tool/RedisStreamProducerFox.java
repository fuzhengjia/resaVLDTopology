package tool;

import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;
import topology.StreamFrame;

import java.util.PriorityQueue;

/**
 * Created by Tom Fu
 * Caution!!  RedisStreamProducerFox is used!
 * Shall use TomVideoStreamReceiverByteArrForLinux for the output!!
 */
public class RedisStreamProducerFox implements Runnable {

    private PriorityQueue<StreamFrame> stream;
    private boolean finished;

    private String host;
    private int port;
    private byte[] queueName;
    private Jedis jedis;

    private long sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    public RedisStreamProducerFox(String host, int port, String queueName) {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);
        this.maxWaitCount = 4;
        this.startFrameID = 1;
        this.sleepTime = 10;
    }

    /**
     *
     * @param host
     * @param port
     * @param queueName
     * @param startFrameID
     * @param maxWaitCount the count for checking the same delayed frame, if expires, skip this frame and move to the next expected frame
     * @param sleepTime  if get a null frame (the expected frame is not received yet, then sleep this amount of time and check again
     */
    public RedisStreamProducerFox(String host, int port, String queueName,
                                  int startFrameID, int maxWaitCount, int sleepTime) {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);
        this.startFrameID = startFrameID;
        this.maxWaitCount = maxWaitCount;
        this.sleepTime = sleepTime;

        System.out.println("Check_init_RedisStreamProducerFox, " + System.currentTimeMillis() +
                ", host: " + this.host + ", port: " + this.port + ", qName: " + this.queueName +
                ", stFrameID: " + this.startFrameID + ", sleepTime: " + this.sleepTime + ", mWaitCnt: " + this.maxWaitCount);
    }

    /**
     * Add frame to the queue if it is fully processed
     */
    public void addFrame(StreamFrame streamFrame) {
        synchronized (stream) {
            stream.add(streamFrame);
        }
    }

    /**
     * Get expected frame from the queue.
     * @return next expected frame, or null if it has not come yet.
     */
    public StreamFrame pollFrame() {
        synchronized (stream) {
            return stream.poll();
        }
    }

    public StreamFrame getPeekFrame() {
        synchronized (stream) {
            return stream.isEmpty() ? null : stream.peek();
        }
    }

    public int getStreamSize() {
        synchronized (stream) {
            return stream.size();
        }
    }

    @Override
    public void run() {
        int currentFrameID = startFrameID;
        int waitCount = 0;
        while (!finished) {
            try {
                StreamFrame peekFrame = getPeekFrame();
                if (peekFrame == null) {
                    //System.out.println("peekFrame == null");
                    Thread.sleep(sleepTime);
                } else {
                    if (peekFrame.frameId <= currentFrameID) {
                        //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") <= currentFrameID: " + currentFrameID);
                        pollFrame();

                    } else if (peekFrame.frameId == (currentFrameID + 1)) {
                        //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") == 1 + currentFrameID: " + currentFrameID);
                        StreamFrame nextFrame = pollFrame();
                        opencv_core.IplImage image = nextFrame.image.asIplImage();
                        opencv_core.Mat matOrg = new opencv_core.Mat(image);
                        Serializable.Mat sMat = new Serializable.Mat(matOrg);
                        jedis.rpush(this.queueName, sMat.toByteArray());

                        System.out.println("finishedAdd: " + System.currentTimeMillis() + ",Fid: " + nextFrame.frameId);
                        currentFrameID++;
                    } else {
                        //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") >> currentFrameID: " + currentFrameID);
                        Thread.sleep(sleepTime);
                        waitCount++;
                        if (waitCount >= maxWaitCount) {
                            System.out.println("frameTimeout, traceID: " + currentFrameID + ", peek: " + peekFrame.frameId + ", qSize: " + stream.size());
                            currentFrameID++;
                            waitCount = 0;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.print("Exception: ");
                e.printStackTrace();
            }
        }
    }
}

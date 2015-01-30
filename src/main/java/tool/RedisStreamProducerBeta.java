package tool;

import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;
import topology.StreamFrame;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayOutputStream;
import java.util.PriorityQueue;

/**
 * This runnable class accepts stream frames, orders them and produces an ordered sequence of frames which is saved
 * to a file. Also displays the results on the canvas as they appear.
 */
public class RedisStreamProducerBeta implements Runnable {
    /**
     * Ordered queue for putting frames in order
     */
    private PriorityQueue<StreamFrame> stream;
    /**
     * Has the last expected frame come?
     */
    private boolean finished;

    //private static final byte[] END = new String("END").getBytes();
    private String host;
    private int port;
    private byte[] queueName;
    //private BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(10000);
    private Jedis jedis;

    private long sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    /**
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId)
     */
    public RedisStreamProducerBeta(String host, int port, String queueName) {

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
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId), with an additional parameter qSize
     */
    public RedisStreamProducerBeta(String host, int port, String queueName,
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
     *
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

    @Override
    public void run() {
        int currentFrameID = startFrameID;
        int waitCount = 0;
        while (!finished) {
            try {
                StreamFrame peekFrame = getPeekFrame();
                if (peekFrame == null) {
                    Thread.sleep(sleepTime);
                } else {
                    if (peekFrame.frameId <= currentFrameID) {
                        pollFrame();
                    } else if (peekFrame.frameId == (currentFrameID + 1)) {
                        StreamFrame nextFrame = pollFrame();
                        opencv_core.IplImage iplImage = nextFrame.image.asIplImage();
                        BufferedImage bufferedImage = iplImage.getBufferedImage();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ImageIO.write(bufferedImage, "JPEG", baos);
                        jedis.rpush(this.queueName, baos.toByteArray());

//                        int size = iplImage.arraySize();
//                        byte[] data = new byte[size];
//                        iplImage.getByteBuffer().get(data);
//                        jedis.rpush(this.queueName, data);

                        System.out.println("finishedAdd: " + System.currentTimeMillis() + ",Fid: " + nextFrame.frameId);
                        currentFrameID++;
                    } else {
                        Thread.sleep(sleepTime);
                        waitCount++;
                        if (waitCount >= maxWaitCount) {
                            System.out.println("frameTimeout, frameID: " + currentFrameID);
                            currentFrameID++;
                            waitCount = 0;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

package tool;

import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;
import topology.StreamFrame;

import java.util.PriorityQueue;

/**
 * This runnable class accepts stream frames, orders them and produces an ordered sequence of frames which is saved
 * to a file. Also displays the results on the canvas as they appear.
 */
public class RedisStreamProducerByteArr implements Runnable {
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
    public RedisStreamProducerByteArr(String host, int port, String queueName) {

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
    public RedisStreamProducerByteArr(String host, int port, String queueName,
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

        System.out.println("Check_init_RedisStreamProducerBeta" +
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

    public int getStreamSize() {
        synchronized (stream) {
            return stream.size();
        }
    }

    @Override
    public void run() {
        int currentFrameID = startFrameID;
        int waitCount = 0;
        opencv_core.IplImage fk = new opencv_core.IplImage();
        while (!finished) {
            try {
                StreamFrame peekFrame = getPeekFrame();
                if (peekFrame == null) {
                    Thread.sleep(sleepTime);
                    //System.out.println("peekFrame == null");
                } else {
                    if (peekFrame.frameId <= currentFrameID) {
                        pollFrame();
                    } else if (peekFrame.frameId == (currentFrameID + 1)) {
                        StreamFrame nextFrame = pollFrame();
//                        opencv_core.IplImage iplImage = nextFrame.image.asIplImage();
//                        //System.out.println("finish asIplImage");
//                        BufferedImage bufferedImage = iplImage.getBufferedImage();
//                        //System.out.println("finish bfferedImage");
                        //ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                        ImageIO.write(bufferedImage, "JPEG", baos);
                        //System.out.println("finish ImageIO");

                        Serializable.Mat sMat = new Serializable.Mat(nextFrame.image);
                        byte[] data = sMat.toByteArray();
                        jedis.rpush(this.queueName, data);

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

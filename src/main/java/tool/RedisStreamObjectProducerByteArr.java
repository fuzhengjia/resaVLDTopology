package tool;

import redis.clients.jedis.Jedis;

import java.util.PriorityQueue;

/**
 * This runnable class accepts stream frames, orders them and produces an ordered sequence of frames which is saved
 * to a file. Also displays the results on the canvas as they appear.
 */
public class RedisStreamObjectProducerByteArr implements Runnable {
    private PriorityQueue<StreamObject> stream;
    private boolean finished;

    private String host;
    private int port;
    private byte[] queueName;
    private Jedis jedis;

    private long sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    /**
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId)
     */
    public RedisStreamObjectProducerByteArr(String host, int port, String queueName) {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        jedis = new Jedis(host, port);
        this.finished = false;

        this.maxWaitCount = 4;
        this.startFrameID = 1;
        this.sleepTime = 10;
    }

    /**
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId), with an additional parameter qSize
     */
    public RedisStreamObjectProducerByteArr(String host, int port, String queueName,
                                            int startFrameID, int maxWaitCount, int sleepTime) {
        this(host, port, queueName);

        this.startFrameID = startFrameID;
        this.maxWaitCount = maxWaitCount;
        this.sleepTime = sleepTime;

        System.out.println("RedisStreamObjectProducerByteArr" +
                ", host: " + this.host + ", port: " + this.port + ", qName: " + this.queueName +
                ", stFrameID: " + this.startFrameID + ", sleepTime: " + this.sleepTime + ", mWaitCnt: " + this.maxWaitCount);
    }

    /**
     * Add frame to the queue if it is fully processed
     */
    public void addFrame(StreamObject streamObject) {
        synchronized (stream) {
            stream.add(streamObject);
        }
    }

    /**
     * Get expected frame from the queue.
     * @return next expected frame, or null if it has not come yet.
     */
    public StreamObject pollFrame() {
        synchronized (stream) {
            return stream.poll();
        }
    }

    public StreamObject getPeekFrame() {
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
        //opencv_core.IplImage fk = new opencv_core.IplImage();
        while (!finished) {
            try {
                StreamObject peekFrame = getPeekFrame();
                if (peekFrame == null) {
                    Thread.sleep(sleepTime);
                    //System.out.println("peekFrame == null");
                } else {
                    if (peekFrame.frameId <= currentFrameID) {
                        pollFrame();
                    } else if (peekFrame.frameId == (currentFrameID + 1)) {
                        StreamObject nextFrame = pollFrame();
                        byte[] data = (byte[])nextFrame.obj;
                        jedis.rpush(this.queueName, data);

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

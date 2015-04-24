package topology;

import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.PriorityQueue;

/**
 * This runnable class accepts stream frames, orders them and produces an ordered sequence of frames which is saved
 * to a file. Also displays the results on the canvas as they appear.
 */
public class RedisStreamProducer implements Runnable {
    /** Ordered queue for putting frames in order */
    private PriorityQueue<StreamFrame> stream;

    /** Currently expected frame */
    //private int nextExpectedFrame;

    /** Has the last expected frame come? */
    private boolean finished;

    //private static final byte[] END = new String("END").getBytes();
    private String host;
    private int port;
    private byte[] queueName;
    //private BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(10000);
    private Jedis jedis;
    /* keep accumulateFrameSize number of frames in the priorityQueue, so that frames can be in defined order */
    private int accumulateFrameSize;

    /** Creates a producer expecting frames in range [firstFrameId, lastFrameId) */
    public RedisStreamProducer(String host, int port, String queueName)  {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);
        this.accumulateFrameSize = 1;
    }

    /** Creates a producer expecting frames in range [firstFrameId, lastFrameId), with an additional parameter qSize */
    public RedisStreamProducer(String host, int port, String queueName, int qSize)  {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);
        this.accumulateFrameSize = qSize;

        System.out.println("Check_init_RedisStreamPro, " + System.currentTimeMillis() +
                ", host: " + this.host + ", port: " + this.port + ", qName: " + this.queueName +
                ", accumulateFrameSize: " + this.accumulateFrameSize);
    }

    /** Add frame to the queue if it is fully processed */
    public void addFrame(StreamFrame streamFrame) {
        synchronized (stream) {
            stream.add(streamFrame);
        }
    }

    /**
     * Get expected frame from the queue.
     * @return next expected frame, or null if it has not come yet.
     */
    public StreamFrame getNextFrame() {
        synchronized (stream) {
            if (stream.size() >= this.accumulateFrameSize) {
                return stream.poll();
            } else {
                return null;
            }
        }
    }


    @Override
    public void run() {
        long count = 0;
        while (!finished){
            try {
                StreamFrame nextFrame = null;
                if ( (nextFrame = getNextFrame()) != null ) {
                    long start = System.currentTimeMillis();
                    //TODO: caution, the average processing time for each frame is about 20ms, which could be a bottleneck at certain cases.
                    opencv_core.IplImage iplImage = nextFrame.image.asIplImage();
                    BufferedImage bufferedImage = iplImage.getBufferedImage();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(bufferedImage, "JPEG", baos);
                    jedis.rpush(this.queueName, baos.toByteArray());
                    //System.out.println("ST: " + (System.currentTimeMillis() - start)
                    //        + System.currentTimeMillis() + ","  + ++count + ",Fid: " + nextFrame.frameId);
                    System.out.println("finishedAdd: " + System.currentTimeMillis() + ",Fid: " + nextFrame.frameId);
                } else {
                    // if expected frame is not there yet, wait and try again.
                    Thread.sleep(10);
                    //System.out.println("STEmpty: " + System.currentTimeMillis() + count);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

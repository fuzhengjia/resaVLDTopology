package topology;

import logodetection.Debug;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.javacv.FrameRecorder;
import org.bytedeco.javacv.OpenCVFrameRecorder;

import java.util.PriorityQueue;

/**
 * This runnable class accepts stream frames, orders them and produces an ordered sequence of frames which is saved
 * to a file. Also displays the results on the canvas as they appear.
 */
public class StreamProducer implements Runnable {
    /** Ordered queue for putting frames in order */
    private PriorityQueue<StreamFrame> stream;
    /** Entity for producing a stream */
    private FrameRecorder recorder;
    /** first and last frame to expect */
    final int firstFrameId, lastFrameId;

    /** Currently expected frame */
    private int nextExpectedFrame;

    /** Has the last expected frame come? */
    private boolean finished;

    /** Canvas for displaying frames on the screen */
    //private CanvasFrame canvasFrame;




    /** Creates a producer expecting frames in range [firstFrameId, lastFrameId) */
    public StreamProducer(int firstFrameId, int lastFrameId, String filename) throws FrameRecorder.Exception {


        stream = new PriorityQueue<>();
        this.firstFrameId = firstFrameId;
        this.lastFrameId = lastFrameId;
        nextExpectedFrame = firstFrameId;

        recorder = FrameRecorder.createDefault(filename, 728, 408);
        recorder.setFrameRate(25);
        recorder.setVideoQuality(1.0);
        recorder.start();

        //canvasFrame = new CanvasFrame("View");
        finished = false;
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
            if (stream.isEmpty() || stream.peek().frameId != nextExpectedFrame) return null;
            nextExpectedFrame ++;
            if (nextExpectedFrame == lastFrameId)
                finished = true;
            return stream.poll();
        }
    }


    @Override
    public void run() {
        while (!finished) {
            try {
                StreamFrame nextFrame = null;
                if ( (nextFrame = getNextFrame()) != null ) {
                    opencv_core.IplImage image = nextFrame.image.asIplImage();
                    recorder.record(image);
                    //canvasFrame.showImage(image);
                    if (finished) {
                        recorder.stop();
                        recorder.release();
                        //canvasFrame.dispose();
                        if (Debug.timer)
                            System.out.println("TIME=" + System.currentTimeMillis());
                    }
                } else {
                    // if expected frame is not there yet, wait and try again.
                    Thread.sleep(40);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (FrameRecorder.Exception e) {
                e.printStackTrace();
            }
        }
    }
}

package tool;

import org.bytedeco.javacpp.opencv_core;

/**
 * The class wrapping the output of the topology - a frame with a list of rectangles found on it.
 * frames are ordered by their frame id.
 */
public class StreamObject implements Comparable<StreamObject> {
    final public int frameId;
    final public Object obj;

    /**
     * creates a StreamFrame with given id, image matrix and list of rectangles corresponding to the detected logos.
     * @param frameId
     * @param obj
     */
    public StreamObject(int frameId, Object obj) {
        this.frameId = frameId;
        this.obj = obj;
    }

    @Override
    public int compareTo(StreamObject o) {
        return frameId - o.frameId;
    }
}

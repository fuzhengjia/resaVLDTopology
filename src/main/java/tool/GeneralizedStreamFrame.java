package tool;

import org.bytedeco.javacpp.opencv_core;

import java.util.Objects;

/**
 * The class wrapping the output of the topology - a frame with a list of rectangles found on it.
 * frames are ordered by their frame id.
 */
public class GeneralizedStreamFrame implements Comparable<GeneralizedStreamFrame> {
    final public int frameId;
    public Object data;

    /**
     * creates a StreamFrame with given id, image matrix and list of rectangles corresponding to the detected logos.
     * @param frameId
     * @param obj
     */
    public GeneralizedStreamFrame(int frameId, Object obj) {
        this.frameId = frameId;
        this.data = obj;
    }

    @Override
    public int compareTo(GeneralizedStreamFrame o) {
        return frameId - o.frameId;
    }
}

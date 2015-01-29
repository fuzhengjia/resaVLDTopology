package tool;

import org.bytedeco.javacpp.opencv_core;

/**
 * The class wrapping the output of the topology - a frame with a list of rectangles found on it.
 * frames are ordered by their frame id.
 */
public class IplImageStream implements Comparable<IplImageStream> {
    final public int frameId;
    final public opencv_core.IplImage image;

    /**
     * creates a StreamFrame with given id, image matrix and list of rectangles corresponding to the detected logos.
     * @param frameId
     * @param image
     */
    public IplImageStream(int frameId, opencv_core.IplImage image) {
        this.frameId = frameId;
        this.image = image;
    }

    @Override
    public int compareTo(IplImageStream o) {
        return frameId - o.frameId;
    }
}

package generateTraj;

import org.bytedeco.javacpp.opencv_core;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class PointDesc {
    public float[] mbhX;
    public float[] mbhY;
    public opencv_core.CvPoint2D32f point;

    PointDesc(DescInfo mbhInfo, opencv_core.CvPoint2D32f point){
        mbhX = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        mbhY = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        this.point = new opencv_core.CvPoint2D32f(point);
        this.point.put(point);
    }
}

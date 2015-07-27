package generateTraj;

import org.bytedeco.javacpp.opencv_core;
import topology.Serializable;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class PointDescWithFeature implements java.io.Serializable{
    public float[] mbhX;
    public float[] mbhY;
    public float[] hog;
    public Serializable.CvPoint2D32f sPoint;

    PointDescWithFeature(DescInfo mbhInfo, DescInfo hogInfo, opencv_core.CvPoint2D32f point){
        mbhX = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        mbhY = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        hog = new float[hogInfo.nxCells * hogInfo.nyCells * hogInfo.nBins];
        this.sPoint = new Serializable.CvPoint2D32f(point);
    }

    PointDescWithFeature(DescInfo mbhInfo, DescInfo hogInfo, Serializable.CvPoint2D32f point) {
        mbhX = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        mbhY = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        hog = new float[hogInfo.nxCells * hogInfo.nyCells * hogInfo.nBins];
        this.sPoint = new Serializable.CvPoint2D32f(point);
    }

    PointDescWithFeature(){};
}
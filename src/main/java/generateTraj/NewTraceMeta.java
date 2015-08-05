package generateTraj;

import tool.Serializable;

import java.util.List;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class NewTraceMeta implements java.io.Serializable {
    public String traceID;//useless
    public Serializable.CvPoint2D32f firstPoint;
    public Serializable.CvPoint2D32f nextPoint;

    public NewTraceMeta(String traceID, Serializable.CvPoint2D32f firstPoint) {
        this.traceID = traceID;
        this.firstPoint = new Serializable.CvPoint2D32f(firstPoint);
        this.nextPoint = null;
    }

    public NewTraceMeta(String traceID, Serializable.CvPoint2D32f firstPoint, Serializable.CvPoint2D32f nextPoint) {
        this.traceID = traceID;
        this.firstPoint = new Serializable.CvPoint2D32f(firstPoint);
        this.nextPoint = new Serializable.CvPoint2D32f(nextPoint);;
    }

    public int getTargetTaskID(List<Integer> taskList) {
        int size = taskList.size();
        if (traceID == null) {
            throw new NullPointerException("traceID is null");
        }
        return taskList.get(Math.abs(traceID.hashCode()) % size);
    }

    public int getTargetTaskIndex(List<Integer> taskList) {
        int size = taskList.size();
        if (traceID == null) {
            throw new NullPointerException("traceID is null");
        }
        return Math.abs(traceID.hashCode()) % size;
    }
}


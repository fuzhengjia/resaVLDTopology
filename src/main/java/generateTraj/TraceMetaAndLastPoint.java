package generateTraj;

import tool.Serializable;

import java.util.List;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class TraceMetaAndLastPoint implements java.io.Serializable {
    public String traceID;//useless
    public Serializable.CvPoint2D32f lastPoint;

    public TraceMetaAndLastPoint(String traceID, Serializable.CvPoint2D32f point) {
        this.traceID = traceID;
        this.lastPoint = new Serializable.CvPoint2D32f(point);
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


package generateTraj;

import java.util.LinkedList;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class TraceRecord implements java.io.Serializable {
    public String traceID;//useless
    public int height;
    public int width;

    public LinkedList<PointDesc> pointDescs;

    public TraceRecord(String traceID, int height, int width, PointDesc point){
        this.traceID = traceID;
        this.pointDescs = new LinkedList<>();
        this.pointDescs.addLast(point);
        this.height = height;
        this.width = width;
    }
}

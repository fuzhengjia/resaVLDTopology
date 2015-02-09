package generateTraj;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class TraceRecord implements java.io.Serializable {
    public String traceID;//useless

    public List<PointDesc> pointDescs;

    public TraceRecord(String traceID, PointDesc point){
        this.traceID = traceID;
        this.pointDescs = new ArrayList<>();
        this.pointDescs.add(point);
    }
}

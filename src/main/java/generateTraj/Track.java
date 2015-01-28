package generateTraj;

import java.util.LinkedList;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class Track {
    public int maxNPoints;
    public LinkedList<PointDesc> pointDescs = new LinkedList<>();

    public Track(int maxNPoints){
        this.maxNPoints = maxNPoints;
    }

    public void addPointDesc(PointDesc point){
        this.pointDescs.addLast(point);
        if (this.pointDescs.size() > (maxNPoints + 2)){
            pointDescs.removeFirst();
        }
    }
}

package generateTraj;

/**
 * Created by Tom.fu on 28/11/2014.
 */
public class TrackerInfo {
    public int trackLength; // length of the trajectory
    public int initGap; // initial gap for feature detection

    public TrackerInfo(int track_length, int init_gap){
        this.trackLength = track_length;
        this.initGap = init_gap;
    }
}

package generateTraj;

/**
 * Created by Tom.fu on 4/2/2015.
 */
public class LastPoint implements java.io.Serializable {

    private int x;
    private int y;
    private int w;
    private int h;

    public LastPoint(int x, int y, int w, int h) {
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
    }

    public int getX() {
        return this.x;
    }

    public int getY() {
        return this.y;
    }

    public int getW() {
        return this.w;
    }

    public int getH() {
        return this.h;
    }

    static public int calCountersIndexForNewTrace(int j, int i, int width){
        return (i * width + j);
    }

    static public int calCountersIndexForRenewTrace(int x, int y, int width){
        return (y * width + x);
    }

}

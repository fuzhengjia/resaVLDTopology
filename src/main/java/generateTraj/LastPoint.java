package generateTraj;

/**
 * Created by Tom.fu on 4/2/2015.
 */
public class LastPoint implements java.io.Serializable{

    private int x;
    private int y;
    private int w;
    private int h;

    public LastPoint(int x, int y, int w, int h){
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
    }

    public int getX (){return this.x;}
    public int getY (){return this.y;}
    public int getW (){return this.w;}
    public int getH (){return this.h;}

    private void setX (int x){this.x = x;}
    private void setY (int y){this.y = y;}
    private void setW (int w){this.w = w;}
    private void setH (int h){this.h = h;}

    public String getFieldString() {
        return (this.x + "-" + this.y);
    }
}

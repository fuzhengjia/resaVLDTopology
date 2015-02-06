package generateTraj;

/**
 * Created by Tom.fu on 4/2/2015.
 */
public class TwoInteger implements java.io.Serializable{

    private int val1;
    private int val2;

    public TwoInteger(int v1, int v2){
        this.val1 = v1;
        this.val2 = v2;
    }

    public int getVal1 (){return this.val1;}
    public int getVal2 (){return this.val2;}

    private void setVal1 (int v){this.val1 = v;}
    private void setVal2 (int v){this.val2 = v;}

    @Override
    public int hashCode() {
        return (this.val1* 31 + this.val2 * 17);
    }
}

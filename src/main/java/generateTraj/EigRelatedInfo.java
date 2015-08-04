package generateTraj;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Tom.fu on 4/2/2015.
 * Notice, int eigWidth = cvFloor(frameWidth / min_distance);  int eigHeight = cvFloor(frameHeight / min_distance);
 * they shall not be same, in eigInfo, we suggest only store frameWidth and frameHeight
 * and then use the two formula to calculate eigWidth and eigHeight!!!
 */
public class EigRelatedInfo implements java.io.Serializable, KryoSerializable {

    private int width;
    private int height;
    private int offset;
    private double threshold;

    public EigRelatedInfo() {
    }

    public EigRelatedInfo(int w, int h, int o, double threshold) {
        this.width = w;
        this.height = h;
        this.offset = o;
        this.threshold = threshold;
    }

    public int getW() {
        return this.width;
    }

    public int getH() {
        return this.height;
    }

    public int getOff() {
        return this.offset;
    }

    public double getTh() {
        return this.threshold;
    }

    public void setW(int w) {
        this.width = w;
    }

    public void setH(int h) {
        this.height = h;
    }

    public void setOff(int o) {
        this.offset = o;
    }

    public void setTh(double th) {
        this.threshold = th;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(this.width);
        output.writeInt(this.height);
        output.writeInt(this.offset);
        output.writeDouble(this.threshold);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.width = input.readInt();
        this.height = input.readInt();
        this.offset = input.readInt();
        this.threshold = input.readDouble();
    }


}

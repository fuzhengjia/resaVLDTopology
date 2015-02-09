package generateTraj;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Tom.fu on 4/2/2015.
 */
public class NewDensePoint implements java.io.Serializable, KryoSerializable {

    private int x;
    private int y;
    private int x_j;
    private int y_i;

    public NewDensePoint(int x, int y, int x_j, int y_i) {
        this.x = x;
        this.y = y;
        this.x_j = x_j;
        this.y_i = y_i;
    }

    public int getX() {
        return this.x;
    }
    public int getY() {
        return this.y;
    }
    public int getX_J() { return this.x_j;}
    public int getY_I() {
        return this.y_i;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(this.x);
        output.writeInt(this.y);
        output.writeInt(this.x_j);
        output.writeInt(this.y_i);
    }


    @Override
    public void read(Kryo kryo, Input input) {
        this.x = input.readInt();
        this.y = input.readInt();
        this.x_j = input.readInt();
        this.y_i = input.readInt();
    }
}

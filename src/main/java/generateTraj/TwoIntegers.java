package generateTraj;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Tom.fu on 4/2/2015.
 */
public class TwoIntegers implements java.io.Serializable, KryoSerializable {

    private int v1;
    private int v2;

    public TwoIntegers(){}

    public TwoIntegers(int v1, int v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public int getV1() {
        return this.v1;
    }

    public int getV2() {
        return this.v2;
    }

    public void setV1(int v) { this.v1 = v; }

    public void setV2(int v) { this.v2 = v; }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(this.v1);
        output.writeInt(this.v2);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.v1 = input.readInt();
        this.v2 = input.readInt();
    }


}

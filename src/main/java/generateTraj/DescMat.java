package generateTraj;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Tom.fu on 3/12/2014.
 */
public class DescMat implements KryoSerializable, java.io.Serializable {

    public int width;
    public int height;
    public int nBins;
    public float[] desc;

    public DescMat() {
    }

    public DescMat(int height, int width, int nBins) {
        this.height = height;
        this.width = width;
        this.nBins = nBins;
        this.desc = new float[height * width * nBins];
//        for (int i = 0; i < this.desc.length; i ++){
//            this.desc[i] = 0;
//        }
    }


    @Override
    public void read(Kryo kryo, Input input) {
        this.width = input.readInt();
        this.height = input.readInt();
        this.nBins = input.readInt();
        int size = input.readInt();
        this.desc = new float[size];
        for (int i = 0; i < desc.length; i++) {
            desc[i] = input.readFloat();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(width);
        output.writeInt(height);
        output.writeInt(nBins);
        output.writeInt(desc.length);
        for (int i = 0; i < desc.length; i++) {
            output.writeFloat(desc[i]);
        }
    }
}

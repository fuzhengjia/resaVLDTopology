package generateTraj;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Tom.fu on 3/12/2014.
 */
public class DescMat implements java.io.Serializable {

    public int width;
    public int height;
    public int nBins;
    public float[] desc;

    public DescMat(int height, int width, int nBins) {
        this.height = height;
        this.width = width;
        this.nBins = nBins;
        this.desc = new float[height * width * nBins];
        for (int i = 0; i < this.desc.length; i ++){
            this.desc[i] = 0;
        }
    }
}

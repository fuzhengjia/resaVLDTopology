package generateTraj;

/**
 * Created by Tom.fu on 3/12/2014.
 */
public class DescMat {

    public int height;
    public int width;
    public int nBins;
    public float[] desc;

    public DescMat(int height, int width, int nBins){
        this.height = height;
        this.width = width;
        this.nBins = nBins;
        this.desc = new float[height*width*nBins];
        for (int i = 0; i < this.desc.length; i ++){
            this.desc[i] = 0;
        }
    }
}

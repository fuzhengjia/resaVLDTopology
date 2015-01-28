package generateTraj;

/**
 * Created by Tom.fu on 28/11/2014.
 */
public class DescInfo {
    public int nBins; // number of bins for vector quantization
    public int fullOrientation; // 0: 180 degree; 1: 360 degree
    public int norm; // 1: L1 normalization; 2: L2 normalization
    public float threshold; //threshold for normalization
    public int flagThre; // whether thresholding or not
    public int nxCells; // number of cells in x direction
    public int nyCells;
    public int ntCells;
    public int dim; // dimension of the descriptor
    public int blockHeight; // size of the block for computing the descriptor
    public int blockWidth;

    public DescInfo(int nBins, int flag, int orientation, int size, int nxy_cell, int nt_cell, float min_flow){
        this.nBins = nBins;
        this.fullOrientation = orientation;
        this.norm = 2;
        this.threshold = min_flow;
        this.flagThre = flag;
        this.nxCells = nxy_cell;
        this.nyCells = nxy_cell;
        this.ntCells = nt_cell;
        this.dim = this.nBins * this.nxCells * this.nyCells;
        this.blockHeight = size;
        this.blockWidth = size;
    }
}

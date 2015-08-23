package GmmModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tom.fu on 19/8/2015.
 */
public class GmmData {
    public int numCluster;
    public int numDimension;
    public int fvLength;
    public float[] means;
    public float[] covs;
    public float[] priors;
    ///sacrifice space to save computation cost, we pre-compute the sqrt of priors for future use
    public float[] sqrtPriors;

    public GmmData() {
        numCluster = 0;
        numDimension = 0;
        fvLength = 2 * numCluster * numDimension;
        means = null;
        covs = null;
        priors = null;
        sqrtPriors = null;
    }

    public GmmData(String fileName) {
        this();
        String rdLine1 = null;
        String rdLine2 = null;
        String rdLine3 = null;
        String rdLine4 = null;

        try {
            BufferedReader myfile = new BufferedReader(new FileReader(fileName));
            rdLine1 = myfile.readLine();
            rdLine2 = myfile.readLine();
            rdLine3 = myfile.readLine();
            rdLine4 = myfile.readLine();

            myfile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (rdLine1 == null || rdLine2 == null || rdLine3 == null || rdLine4 == null) {
            System.out.println("warning in GmmData(), rdline shall not be null");
        } else {

            String[] rdLineSplit = rdLine1.split(" ");
            this.numDimension = Integer.parseInt(rdLineSplit[0].trim());
            this.numCluster = Integer.parseInt(rdLineSplit[1].trim());
            this.fvLength = 2 * this.numCluster * this.numDimension;
            rdLineSplit = null;

            List<Float> dList = new ArrayList<>();
            rdLineSplit = rdLine2.split(" ");
            for (int i = 0; i < rdLineSplit.length; i++) {
                if (!rdLineSplit[i].trim().isEmpty()) {
                    dList.add(Float.parseFloat(rdLineSplit[i].trim()));
                }
            }

            if (dList.size() != numCluster * numDimension) {
                System.out.println("warning, dList.size() != numCluster * numDimension, " + dList.size() + ", " + numCluster * numDimension);
            }

            this.means = new float[numCluster * numDimension];
            for (int i = 0; i < dList.size(); i++) {
                this.means[i] = dList.get(i).floatValue();
            }

            dList = null;
            rdLineSplit = null;

            dList = new ArrayList<>();
            rdLineSplit = rdLine3.split(" ");
            for (int i = 0; i < rdLineSplit.length; i++) {
                if (!rdLineSplit[i].trim().isEmpty()) {
                    dList.add(Float.parseFloat(rdLineSplit[i].trim()));
                }
            }

            if (dList.size() != numCluster * numDimension) {
                System.out.println("warning, dList.size() != numCluster * numDimension, " + dList.size() + ", " + numCluster * numDimension);
            }

            this.covs = new float[numCluster * numDimension];
            for (int i = 0; i < dList.size(); i++) {
                this.covs[i] = dList.get(i).floatValue();
            }
            dList = null;
            rdLineSplit = null;

            dList = new ArrayList<>();
            rdLineSplit = rdLine4.split(" ");
            for (int i = 0; i < rdLineSplit.length; i++) {
                if (!rdLineSplit[i].trim().isEmpty()) {
                    dList.add(Float.parseFloat(rdLineSplit[i].trim()));
                }
            }

            if (dList.size() != numCluster) {
                System.out.println("warning, dList.size() != numCluster, " + dList.size() + ", " + numCluster);
            }

            this.priors = new float[numCluster];
            this.sqrtPriors = new float[numCluster];
            for (int i = 0; i < dList.size(); i++) {
                this.priors[i] = dList.get(i).floatValue();
                this.sqrtPriors[i] = (float)Math.sqrt(this.priors[i]);
            }
        }
    }
}

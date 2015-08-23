package GmmModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tom.fu on 19/8/2015.
 */
public class PcaData {
    public int aveDim;
    public int eigDim;
    public float[] projMat;
    public float[] aveVec;
    public float[] eigVec;

    public PcaData() {
        aveDim = 0;
        eigDim = 0;
        projMat = null;
        aveVec = null;
        eigVec = null;
    }

    public PcaData(String fileName) {
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
            System.out.println("warning in PcaData(), rdline shall not be null");
        } else {

            String[] rdLineSplit = rdLine1.split(" ");
            this.aveDim = Integer.parseInt(rdLineSplit[0].trim());
            this.eigDim = Integer.parseInt(rdLineSplit[1].trim());
            rdLineSplit = null;

            List<Float> dList = new ArrayList<>();
            rdLineSplit = rdLine2.split(" ");
            for (int i = 0; i < rdLineSplit.length; i++) {
                if (!rdLineSplit[i].trim().isEmpty()) {
                    dList.add(Float.parseFloat(rdLineSplit[i].trim()));
                }
            }

            if (dList.size() != aveDim * eigDim) {
                System.out.println("warning in PcaData(), dList.size() != aveDim * eigDim, " + dList.size() + ", " + aveDim * eigDim);
            }

            this.projMat = new float[aveDim * eigDim];
            for (int i = 0; i < dList.size(); i++) {
                this.projMat[i] = dList.get(i).floatValue();
            }
            ///here we make a transpose of the original matrix for ease of doing matrix multiplication!!!
            ///todo: check accuracy
//            for (int i = 0; i < aveDim; i ++){
//                for (int j = 0; j < eigDim; j ++){
//                    int orgIndex = j * aveDim + i;
//                    int transIndex = i * eigDim + j;
//                    this.projMat[transIndex] = dList.get(orgIndex).floatValue();
//                }
//            }

            dList = null;
            rdLineSplit = null;

            dList = new ArrayList<>();
            rdLineSplit = rdLine3.split(" ");
            for (int i = 0; i < rdLineSplit.length; i++) {
                if (!rdLineSplit[i].trim().isEmpty()) {
                    dList.add(Float.parseFloat(rdLineSplit[i].trim()));
                }
            }

            if (dList.size() != aveDim) {
                System.out.println("warning in PcaData(), dList.size() != aveDim, " + dList.size() + ", " + aveDim);
            }

            this.aveVec = new float[aveDim];
            for (int i = 0; i < dList.size(); i++) {
                this.aveVec[i] = dList.get(i).floatValue();
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

            if (dList.size() != eigDim) {
                System.out.println("warning in PcaData(), dList.size() != eigDim, " + dList.size() + ", " + eigDim);
            }

            this.eigVec = new float[eigDim];
            for (int i = 0; i < dList.size(); i++) {
                this.eigVec[i] = dList.get(i).floatValue();
            }
        }
    }
}

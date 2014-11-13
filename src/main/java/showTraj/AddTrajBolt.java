package showTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import topology.Serializable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

import com.jmatio.io.MatFileReader;
import com.jmatio.types.MLCell;
import com.jmatio.types.MLDouble;

import static org.bytedeco.javacpp.opencv_core.CV_8UC1;
import static org.bytedeco.javacpp.opencv_core.cvMat;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static tool.Constant.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getString;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class AddTrajBolt extends BaseRichBolt {
    OutputCollector collector;

    private ArrayList<ArrayList<Float>> frameTraj = new  ArrayList<ArrayList<Float>>();
    private ArrayList<ArrayList<Float>> frameTrajIpnut = new  ArrayList<ArrayList<Float>>();
    private BufferedReader reader;
    private ArrayList<int[]> groupColor;
    private int maxFrameID;
    private ArrayList<Integer> groupIDs;

    private String path;
    private int repeatCount;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        //String path = "C:\\Users\\Tom.fu\\Desktop\\fromPeiYong\\";
        path = getString(map, "sourceFilePath");
        repeatCount = getInt(map, "repeatCount");
        String file1 = path + "traj_bend_0001_trajectory_group_1.mat";
        String file2 = path + "traj_bend_0001_trajectory_group_2.mat";
        String trajFile = path + "traj_bend_0001.txt";

        try {
            MatFileReader mfr1 = new MatFileReader(file1);
            MLDouble mlData1 = (MLDouble) ((MLCell) mfr1.getMLArray("group_ids")).cells().get(0);
            ArrayList<Integer> group1 = getIndexArrayFromMLDouble(mlData1);

            MatFileReader mfr2 = new MatFileReader(file2);
            MLDouble mlData2 = (MLDouble) ((MLCell) mfr2.getMLArray("group_ids")).cells().get(0);
            ArrayList<Integer> group2 = getIndexArrayFromMLDouble(mlData2);

            int maxGroup1 = group1.stream().max(Integer::compare).get();
            ArrayList<Integer> group2New = (ArrayList<Integer>) group2.stream().map(item -> item + maxGroup1)
                    .collect(Collectors.toList());

            groupIDs = new ArrayList<>();
            groupIDs.addAll(group1);
            groupIDs.addAll(group2);

            int maxGroupID = groupIDs.stream().max(Integer::compare).get();
            //System.out.println("maxGroup1: " + maxGroup1 + ", maxGroupID: " + maxGroupID);
            //System.out.println("group1Cnt: " + group1.size() + ", group2Cnt: "
            //        + group2.size() + ", groups: " + groupIDs.size());

            groupColor = getRandomColor(maxGroupID, 3);

            reader = new BufferedReader(new FileReader(trajFile));
            String rdLine = null;
            while ((rdLine = reader.readLine()) != null) {
                ArrayList<String> rdLineResults = new ArrayList<String>(Arrays.asList(rdLine.split(" ")));
                ArrayList<Float> rdResults = (ArrayList<Float>) rdLineResults.stream().map(item -> Float.valueOf(item))
                        .collect(Collectors.toList());
                frameTrajIpnut.add(rdResults);
            }
            reader.close();
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }

        int maxFrameIDOff = frameTrajIpnut.stream().mapToInt(item->item.get(0).intValue()).reduce(Integer::max).getAsInt();
        frameTraj.addAll(frameTrajIpnut);

        ArrayList<Integer> groupIDClone = new ArrayList<Integer>();
        groupIDClone.addAll(groupIDs);

        for (int  i = 0; i < repeatCount; i ++) {
            maxFrameID = i * maxFrameIDOff;
            ArrayList<ArrayList<Float>> tmp = (ArrayList<ArrayList<Float>>)frameTrajIpnut.stream().map(item ->
            {
                int newID = item.get(0).intValue() + maxFrameID;
                ArrayList<Float> newArray = new ArrayList<Float>();
                newArray.addAll(item);
                newArray.set(0, (float)newID);
                return newArray;
            }).collect(Collectors.toList());
            frameTraj.addAll(tmp);
            groupIDs.addAll(groupIDClone);
        }
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        byte[] imgBytes = (byte[]) tuple.getValueByField(FIELD_FRAME_BYTES);

        opencv_core.IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));

        opencv_core.Mat mat = new opencv_core.Mat(image);
        opencv_core.Mat matNew = new opencv_core.Mat();
        opencv_core.Size size = new opencv_core.Size(640, 480);
        //opencv_imgproc.resize(matOrg, matNew, size);
        opencv_imgproc.resize(mat, matNew, size);

        for (int j = 0; j < 15; j ++){
            int endFrameID = frameId + j;
            int duration = 16 - j;

            ArrayList<Integer> selectedFrameIndex = (ArrayList<Integer>)frameTraj.stream()
                    .filter(item->item.get(0).intValue()==endFrameID).map(item->frameTraj.indexOf(item))
                    .collect(Collectors.toList());

            for (int k : selectedFrameIndex) {
                int gid = groupIDs.get(k);

                int x_prev = frameTraj.get(k).get(7).intValue();
                int y_prev = frameTraj.get(k).get(8).intValue();

                for (int m = 1; m < duration; m ++){
                    int x_curr = frameTraj.get(k).get(7 + m * 2).intValue();
                    int y_curr = frameTraj.get(k).get(8 + m * 2).intValue();

                    //opencv_core.line(finalImage, new opencv_core.Point((int) Q[i][0], (int) Q[i][1]),
                    //new opencv_core.Point((int) Q[(i + 1) % 4][0], (int) Q[(i + 1) % 4][1]), color, 4, 4, 0);
                    opencv_core.line(matNew,
                            new opencv_core.Point(x_prev, y_prev),
                            new opencv_core.Point(x_curr, y_curr),
                            new opencv_core.Scalar(
                                    groupColor.get(gid-1)[0],
                                    groupColor.get(gid-1)[1],
                                    groupColor.get(gid-1)[2], 0));
                    x_prev = x_curr;
                    y_prev = y_curr;
                }
            }
        }
        //opencv_highgui.namedWindow("test", opencv_highgui.WINDOW_AUTOSIZE);
        //opencv_highgui.imshow("test", matNew);
        //opencv_highgui.waitKey(1);

        Serializable.Mat sMat = new Serializable.Mat(matNew);
        collector.emit(STREAM_FRAME_OUTPUT, tuple, new Values(frameId, sMat));
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }

    static ArrayList<Integer> getIndexArrayFromMLDouble(MLDouble input) {
        double[][] temp = input.getArray();
        ArrayList<Integer> ret = new ArrayList<>();
        for (int i = 0; i < temp.length; i++) {
            ret.add((int) temp[i][0]);
        }
        return ret;
    }

    static ArrayList<int[]> getRandomColor(int maxID, int dim) {
        Random rnd = new Random(System.currentTimeMillis());
        ArrayList<int[]> ret = new ArrayList<>();
        for (int i = 0; i < maxID; i++) {
            int[] rgbRnd = new int[dim];
            for (int j = 0; j < dim; j++) {
                rgbRnd[j] = (int) (255 * rnd.nextDouble());
            }
            ret.add(rgbRnd);
        }
        return ret;
    }
}

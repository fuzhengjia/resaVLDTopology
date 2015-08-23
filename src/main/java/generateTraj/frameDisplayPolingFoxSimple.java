package generateTraj;

import GmmModel.GmmData;
import GmmModel.NewMethod;
import GmmModel.PcaData;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tool.Serializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;
import static topology.StormConfigManager.getString;
import static util.ConfigUtil.getInt;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * <p>
 * Strange issue, need to use RedisStreamProducerBeta????
 * In this version, we change some data formats
 */
public class frameDisplayPolingFoxSimple extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, List<float[]>> rawFeatureDataList;
    private HashMap<Integer, Integer> fvCounter;
    private HashMap<Integer, Integer> fvResult;
    private int windowSize;
    private HashMap<Integer, Serializable.Mat> rawFrameMap;
    private HashMap<Integer, Integer> traceMonitor;

    int numDimension = 288;
    int numCluster = 256;
    int fvLength = 2 * numCluster * numDimension / 2;

    int offset = 14;
    int frameRate = 15;

    PcaData hogPca;
    PcaData mbhxPca;
    PcaData mbhyPca;

    GmmData hogGmm;
    GmmData mbhxGmm;
    GmmData mbhyGmm;

    List<float[]> trainingResult;

    private boolean toDebug = true;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rawFeatureDataList = new HashMap<>();
        rawFrameMap = new HashMap<>();
        fvCounter = new HashMap<>();
        fvResult = new HashMap<>();

        traceMonitor = new HashMap<>();
        this.windowSize = getInt(map, "fvWinSize", 75); ///default value 75 = 15 * 5, 15fps for 5 seconds.

        String hogPcaFile = getString(map, "hogPcaFilePath");
        String mbhxPcaFile = getString(map, "mbhxPcaFilePath");
        String mbhyPcaFile = getString(map, "mbhyPcaFilePath");

        String hogGmmFile = getString(map, "hogGmmFilePath");
        String mbhxGmmFile = getString(map, "mbhxGmmFilePath");
        String mbhyGmmFile = getString(map, "mbhyGmmFilePath");

        String trainDataFile = getString(map, "trainDataFilePath");

        hogPca = new PcaData(hogPcaFile);
        mbhxPca = new PcaData(mbhxPcaFile);
        mbhyPca = new PcaData(mbhyPcaFile);

        hogGmm = new GmmData(hogGmmFile);
        mbhxGmm = new GmmData(mbhxGmmFile);
        mbhyGmm = new GmmData(mbhyGmmFile);

        trainingResult = NewMethod.getTrainingResult_float(trainDataFile, fvLength);
    }

    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        IplImage fake = new IplImage();

        if (streamId.equals(STREAM_FRAME_OUTPUT)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            rawFrameMap.computeIfAbsent(frameId, k -> sMat);

        } else if (streamId.equals(STREAM_FRAME_FV)) {
            List<float[]> data = (List<float[]>) tuple.getValueByField(FIELD_FEA_VEC);
            traceMonitor.put(frameId, -1); /// -1 means undefined action.

            int winIndex = (frameId - 1 - offset) / this.windowSize; ///there is an offset between frameID and Window
            if (rawFeatureDataList.containsKey(winIndex)) {
                rawFeatureDataList.get(winIndex).addAll(data);
                fvCounter.computeIfPresent(winIndex, (k, v) -> v + 1);
                System.out.println("frameID: " + frameId + ", winIndex: " + winIndex + ", fvCounter: " + fvCounter.get(winIndex));
                if (fvCounter.get(winIndex) == this.windowSize) {

                    Object[] result = NewMethod.checkNew_float(rawFeatureDataList.get(winIndex), trainingResult,
                            numDimension, hogPca, mbhxPca, mbhyPca, hogGmm, mbhxGmm, mbhyGmm, true);
                    int getClassificationID = (int) result[0];
                    float sim = (float) result[1];

                    rawFeatureDataList.remove(winIndex);
                    fvCounter.remove(winIndex);
                    System.out.println("simframeID: " + frameId + ", winIndex: " + winIndex + ", cResult: " + getClassificationID + ", sim: " + +sim + ", ht.cnt: " + rawFeatureDataList.size());
                    traceMonitor.put(frameId, getClassificationID);
                    fvResult.put(winIndex, getClassificationID);
                }
            } else {
                rawFeatureDataList.put(winIndex, data);
                fvCounter.put(winIndex, 1);
            }
        }

        if (frameId < offset || (rawFrameMap.containsKey(frameId) && traceMonitor.containsKey(frameId))) {

            Mat orgMat = rawFrameMap.get(frameId).toJavaCVMat();
            IplImage frame = orgMat.asIplImage();
            CvFont font = new CvFont();
            cvInitFont(font, CV_FONT_VECTOR0, 0.5f, 0.5f, 0, 1, 8);
            CvPoint showPos = cvPoint(10, 20);
            CvScalar showColor = CV_RGB(0, 0, 0);

            if (frameId < offset + 2 * this.frameRate + 1){
                cvPutText(frame, "Action Detection", showPos, font, showColor);
            }else {
                int adjFrameID = frameId - offset - 2 * this.frameRate - 1; ///window is 75, 0-14, 15-29, 30-44, 45-59, 60-74
                int winIndex = adjFrameID / this.windowSize;
                int secPos = (adjFrameID % this.windowSize) / this.frameRate;

                //3, 2, 1, x, x, 3, 2, 1, x, x,
                if (secPos < 3) {//
                    int showSecondInfor = 3 - secPos;
                    cvPutText(frame, showSecondInfor + " ", showPos, font, showColor);
                } else {
                    int getClassificationID = fvResult.get(winIndex + 1);
                    cvPutText(frame, NewMethod.getClassificationString(getClassificationID), showPos, font, showColor);
                }
                fvResult.remove(winIndex - 3);
            }

            Mat mat = new Mat(frame);
            Serializable.Mat sMat = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMat));
            rawFrameMap.remove(frameId);
            traceMonitor.remove(frameId);
            if (toDebug) {
                System.out.println("FrameDisplay-finishedAdd: " + frameId + ", tCnt: " + traceMonitor.size()
                        + "@" + System.currentTimeMillis());
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}

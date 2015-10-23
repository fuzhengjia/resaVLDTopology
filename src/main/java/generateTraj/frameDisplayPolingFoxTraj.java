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
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.Serializable;
import topology.StormConfigManager;
import util.ConfigUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;
import static topology.StormConfigManager.getListOfStrings;
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
public class frameDisplayPolingFoxTraj extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, List<float[]>> rawFeatureDataList;
    private HashMap<Integer, Integer> fvCounter;
    private HashMap<Integer, Integer> fvResult;
    private HashMap<Integer, Serializable.Mat> rawFrameMap;
    private HashMap<Integer, Integer> featureMonitor;
    private HashMap<Integer, List<List<Serializable.CvPoint2D32f>>> traceData;
    private HashMap<Integer, Integer> traceMonitor;

    List<CvScalar> colorList;

    int numDimension = 288;
    int numCluster = 256;
    int fvLength = 2 * numCluster * numDimension / 2;

    int maxTrackerLength;
    int frameRate;
    int windowInSeconds;
    int windowInFrames;
    int resultLastSeconds;
    int countDownSeconds;

    int procWidth;
    int procHeight;
    int outputW;
    int outputH;
    int inHeight;
    int inWidth;

    float adjRatioX = 1.0f;
    float adjRatioY = 1.0f;

    PcaData hogPca;
    PcaData mbhxPca;
    PcaData mbhyPca;

    GmmData hogGmm;
    GmmData mbhxGmm;
    GmmData mbhyGmm;

    List<float[]> trainingResult;
    List<String> actionNameList;

    String traceAggBoltNameString;
    int traceAggBoltTaskNumber;
    private boolean toDebug = false;

    public frameDisplayPolingFoxTraj(String traceAggBoltNameString){
        this.traceAggBoltNameString = traceAggBoltNameString;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rawFeatureDataList = new HashMap<>();
        rawFrameMap = new HashMap<>();
        fvCounter = new HashMap<>();
        fvResult = new HashMap<>();
        traceData = new HashMap<>();
        traceMonitor = new HashMap<>();
        this.traceAggBoltTaskNumber = topologyContext.getComponentTasks(traceAggBoltNameString).size();

        featureMonitor = new HashMap<>();
        this.frameRate = getInt(map, "frameRate", 15);
        this.windowInSeconds = getInt(map, "windowInSeconds", 5); ///windowInFrames = windowInSeconds * frameRate
        this.windowInFrames = this.windowInSeconds * this.frameRate;
        /// [3][2][1][R][R]...[3][2][1][R][R]
        this.resultLastSeconds = getInt(map, "resultLastSeconds", 2); /// Countdown seconds = windowInseconds - resultLastSeconds
        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);  ///offset + 1
        this.countDownSeconds = this.windowInSeconds - this.resultLastSeconds;

        String hogPcaFile = getString(map, "hogPcaFilePath");
        String mbhxPcaFile = getString(map, "mbhxPcaFilePath");
        String mbhyPcaFile = getString(map, "mbhyPcaFilePath");

        String hogGmmFile = getString(map, "hogGmmFilePath");
        String mbhxGmmFile = getString(map, "mbhxGmmFilePath");
        String mbhyGmmFile = getString(map, "mbhyGmmFilePath");

        String trainDataFile = getString(map, "trainDataFilePath");

        procWidth = StormConfigManager.getInt(map, "procWidth");
        procHeight = StormConfigManager.getInt(map, "procHeight");
        inWidth = StormConfigManager.getInt(map, "inWidth");
        inHeight = StormConfigManager.getInt(map, "inHeight");
        outputW = StormConfigManager.getInt(map, "outputW");
        outputH = StormConfigManager.getInt(map, "outputH");

        adjRatioX = (float)inWidth / (float)procWidth;
        adjRatioY = (float)inHeight / (float)procHeight;

        hogPca = new PcaData(hogPcaFile);
        mbhxPca = new PcaData(mbhxPcaFile);
        mbhyPca = new PcaData(mbhyPcaFile);

        hogGmm = new GmmData(hogGmmFile);
        mbhxGmm = new GmmData(mbhxGmmFile);
        mbhyGmm = new GmmData(mbhyGmmFile);

        trainingResult = NewMethod.getTrainingResult_float(trainDataFile, fvLength);
        actionNameList = getListOfStrings(map, "actionNames");

        toDebug = ConfigUtil.getBoolean(map, "debugTopology", false);
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
            featureMonitor.put(frameId, -1); /// -1 means undefined action.

            ///there is an offset between frameID and Window, and this.maxTrackerLength = offset + 1;
            int winIndex = (frameId - this.maxTrackerLength) / this.windowInFrames;
            if (rawFeatureDataList.containsKey(winIndex)) {
                rawFeatureDataList.get(winIndex).addAll(data);
                fvCounter.computeIfPresent(winIndex, (k, v) -> v + 1);
//                System.out.println("frameID: " + frameId + ", winIndex: " + winIndex + ", fvCounter: " + fvCounter.get(winIndex));
                if (fvCounter.get(winIndex) == this.windowInFrames) {

                    Object[] result = NewMethod.checkNew_float(rawFeatureDataList.get(winIndex), trainingResult,
                            numDimension, hogPca, mbhxPca, mbhyPca, hogGmm, mbhxGmm, mbhyGmm, toDebug);
                    int getClassificationID = (int) result[0];
                    float sim = (float) result[1];

                    rawFeatureDataList.remove(winIndex);
                    fvCounter.remove(winIndex);
//                    System.out.println("simframeID: " + frameId + ", winIndex: " + winIndex + ", cResult: " + getClassificationID + ", sim: " + +sim + ", ht.cnt: " + rawFeatureDataList.size());
                    featureMonitor.put(frameId, getClassificationID);
                    fvResult.put(winIndex, getClassificationID);
                }
            } else {
                rawFeatureDataList.put(winIndex, data);
                fvCounter.put(winIndex, 1);
            }
        } else if (streamId.equals(STREAM_PLOT_TRACE)) {
            List<List<Serializable.CvPoint2D32f>> traceRecords = (List<List<Serializable.CvPoint2D32f>>) tuple.getValueByField(FIELD_TRACE_RECORD);
            if (!traceMonitor.containsKey(frameId)){
                traceMonitor.put(frameId, 1);
                traceData.put(frameId, traceRecords);
            }else {
                traceMonitor.computeIfPresent(frameId, (k,v)->v+1);
                traceData.get(frameId).addAll(traceRecords);
            }
        }

        //TODO: here is a bug!! this if has some problem!
        //todo, try this:
        //if (frameId < this.maxTrackerLength || (rawFrameMap.containsKey(frameId) && traceMonitor.containsKey(frameId))) {
        if ((rawFrameMap.containsKey(frameId) && traceData.containsKey(frameId) && traceMonitor.get(frameId) == this.traceAggBoltTaskNumber)
                && (frameId < this.maxTrackerLength || featureMonitor.containsKey(frameId))){

            Mat orgMat = rawFrameMap.get(frameId).toJavaCVMat();
            IplImage orgFrame = orgMat.asIplImage();

            ///add action detection results
            IplImage actFrame = cvCreateImage(cvSize(this.outputW, this.outputH), 8, 3);
            opencv_imgproc.cvResize(orgFrame, actFrame, opencv_imgproc.CV_INTER_AREA);

            CvFont font = new CvFont();
            cvInitFont(font, CV_FONT_VECTOR0, 1.2f, 1.2f, 0, 2, 8);
            CvPoint showPos = cvPoint(5, 40);
            //CvScalar showColor = CV_RGB(255, 127, 39);
            CvScalar showColor = CvScalar.YELLOW;
            //CvPoint showPos2 = cvPoint(5, 465);
            CvPoint showPos2 = cvPoint(5, this.outputH - 15);

            if (frameId < maxTrackerLength + resultLastSeconds * frameRate) {
                cvPutText(actFrame, "Action Detection", showPos, font, showColor);
            } else {
                int adjFrameID = frameId - maxTrackerLength - resultLastSeconds * frameRate; ///window is 75, 0-14, 15-29, 30-44, 45-59, 60-74
                int winIndex = adjFrameID / this.windowInFrames;
                int secPos = (adjFrameID % this.windowInFrames) / this.frameRate;

                //3, 2, 1, x, x, 3, 2, 1, x, x,
                if (secPos < this.countDownSeconds) {//
                    int showSecondInfo = this.countDownSeconds - secPos;
                    int t = this.windowInSeconds - showSecondInfo;
                    int percent = t * 100 / this.windowInSeconds;
                    cvPutText(actFrame, "Detecting action... " + percent + "%", showPos2, font, showColor);
                } else {
                    int getClassificationID = fvResult.containsKey(winIndex) == true ? fvResult.get(winIndex) : -1;
                    cvPutText(actFrame, "Action: " + NewMethod.getClassificationString(getClassificationID, actionNameList), showPos, font, showColor);
                }
                fvResult.remove(winIndex - 3);
            }

            ///draw trajectories
            List<List<Serializable.CvPoint2D32f>> traceRecords = traceData.get(frameId);
            for (List<Serializable.CvPoint2D32f> trace : traceRecords) {
                float length = trace.size();
                float point0_x = trace.get(0).x();
                float point0_y = trace.get(0).y();
                CvPoint2D32f point0 = new CvPoint2D32f();
                point0.x(point0_x * adjRatioX);
                point0.y(point0_y * adjRatioY);

                float jIndex = 0;
                for (int jj = 1; jj < length; jj++, jIndex++) {
                    float point1_x = trace.get(jj).x();
                    float point1_y = trace.get(jj).y();
                    CvPoint2D32f point1 = new CvPoint2D32f();
                    point1.x(point1_x * adjRatioX);
                    point1.y(point1_y * adjRatioY);

                    cvLine(orgFrame, cvPointFrom32f(point0), cvPointFrom32f(point1),
                            CV_RGB(0, cvFloor(255.0 * (jIndex + 1.0) / length), 0), 1, 8, 0);
                    point0 = point1;
                }
            }

            IplImage trajFrame = cvCreateImage(cvSize(this.outputW, this.outputH), 8, 3);
            opencv_imgproc.cvResize(orgFrame, trajFrame, opencv_imgproc.CV_INTER_AREA);

            opencv_core.Mat trajMat = new opencv_core.Mat(trajFrame);
            opencv_core.Mat actMat = new opencv_core.Mat(actFrame);

            opencv_core.Mat combineMat = new opencv_core.Mat();
            opencv_core.Size size = new opencv_core.Size(outputW, 2*outputH);
            opencv_imgproc.resize(actMat, combineMat, size);

            opencv_core.Mat dst_roi1 = new opencv_core.Mat(combineMat, new opencv_core.Rect(0, 0, outputW, outputH));
            trajMat.copyTo(dst_roi1);
            opencv_core.Mat dst_roi2 = new opencv_core.Mat(combineMat, new opencv_core.Rect(0, outputH, outputW, outputH));
            actMat.copyTo(dst_roi2);

            Serializable.Mat sMat = new Serializable.Mat(combineMat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMat));
            rawFrameMap.remove(frameId);
            featureMonitor.remove(frameId);
            traceMonitor.remove(frameId);
            traceData.remove(frameId);
            if (toDebug) {
                System.out.println("FrameDisplay-finishedAdd: " + frameId + ", tCnt: " + featureMonitor.size()
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

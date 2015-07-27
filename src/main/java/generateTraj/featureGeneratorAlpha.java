package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import topology.Serializable;
import util.ConfigUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu
 * Input is MbhMat and HogMat for a window of 15-20 frames, and the trace records with length = 16
 * Output the feature vectors for each trace.
 */
public class featureGeneratorAlpha extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, DescMat[]> desMatMap;
    private HashMap<Integer, List<List<PointDesc>>> traceData;
    private HashMap<Integer, Integer> traceMonitor;

    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static float[] fscales;
    static int ixyScale = 0;

    static int patch_size = 32;
    static int nxy_cell = 2;
    static int nt_cell = 3;
    static float min_flow = 0.4f * 0.4f;
    static int dimension = 32;

    int maxTrackerLength;
    DescInfo hogInfo, mbhInfo;

    String traceAggBoltNameString;
    int traceAggBoltTaskNumber;

    public featureGeneratorAlpha(String traceAggBoltNameString) {
        this.traceAggBoltNameString = traceAggBoltNameString;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        traceData = new HashMap<>();
        traceMonitor = new HashMap<>();
        desMatMap = new HashMap<>();

        this.traceAggBoltTaskNumber = topologyContext.getComponentTasks(traceAggBoltNameString).size();

        fscales = new float[scale_num];
        for (int i = 0; i < scale_num; i++) {
            fscales[i] = (float) Math.pow(scale_stride, i);
        }

        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);
        this.hogInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
        this.mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);

        IplImage fake = new IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        if (frameId == 0) {
            collector.ack(tuple);
            return;
        }
        //System.out.println("receive tuple, frameID: " + frameId + ", streamID: " + streamId);
        if (streamId.equals(STREAM_FEATURE_FLOW)) {
            DescMat[] feaMat = (DescMat[]) tuple.getValueByField(FIELD_MBH_HOG_MAT);

            desMatMap.computeIfAbsent(frameId, k -> feaMat);

        } else if (streamId.equals(STREAM_FEATURE_TRACE)) {
            List<List<PointDesc>> traceRecords = (List<List<PointDesc>>) tuple.getValueByField(FIELD_TRACE_RECORD);
            if (!traceMonitor.containsKey(frameId)) {
                traceMonitor.put(frameId, 1);
                traceData.put(frameId, traceRecords);
            } else {
                traceMonitor.computeIfPresent(frameId, (k, v) -> v + 1);
                traceData.get(frameId).addAll(traceRecords);
            }
        }

        boolean allReady = true;
        for (int i = frameId - this.maxTrackerLength - 1; i < frameId; i ++){
            if (this.desMatMap.containsKey(i) == false){
                allReady = false;
            }
        }

        if (desMatMap.containsKey(frameId) && traceData.containsKey(frameId)
                && traceMonitor.get(frameId) == this.traceAggBoltTaskNumber && allReady) {

            collector.emit(STREAM_CACHE_CLEAN, new Values(frameId));

            List<List<PointDesc>> traceRecords = traceData.get(frameId);
            List<float[]> traceFeatures = new ArrayList<>();
            int t_stride = cvFloor(this.maxTrackerLength / this.nt_cell);

            for (List<PointDesc> trace : traceRecords) {
                if (trace.size() != this.maxTrackerLength + 1) {
                    throw new IllegalArgumentException("trace.size() != this.maxTrackerLength + 1, trace.size() = " + trace.size());
                }

//                float[] hogFeatures = new float[this.nt_cell * this.dimension];
//                float[] mbhXFeatures = new float[this.nt_cell * this.dimension];
//                float[] mbhYFeatures = new float[this.nt_cell * this.dimension];
                float[] allFeatures = new float[this.nt_cell * this.dimension * 3];
                int iDescIndex = 0;
                for (int n = 0; n < this.nt_cell; n++) {
                    float[] hogVec = new float[this.dimension];
                    float[] mbhxVec = new float[this.dimension];
                    float[] mbhyVec = new float[this.dimension];
                    for (int m = 0; m < this.dimension; m++) {
                        hogVec[m] = 0;
                        mbhxVec[m] = 0;
                        mbhyVec[m] = 0;
                    }
                    for (int t = 0; t < t_stride; t++, iDescIndex++) {
                        int fID = frameId - trace.size() + iDescIndex;
                        DescMat[] desMat = desMatMap.get(fID);
                        DescMat mbhMatX = desMat[0];
                        DescMat mbhMatY = desMat[1];
                        DescMat hogMat = desMat[2];

                        CvScalar rect = helperFunctions.getRect(trace.get(iDescIndex).sPoint, cvSize(hogMat.width, hogMat.height), hogInfo);
                        float[] mbhX = helperFunctions.getDesc(mbhMatX, rect, mbhInfo);
                        float[] mbhY = helperFunctions.getDesc(mbhMatY, rect, mbhInfo);
                        float[] hog = helperFunctions.getDesc(hogMat, rect, hogInfo);

                        for (int m = 0; m < this.dimension; m++) {
                            hogVec[m] += hog[m];
                            mbhxVec[m] += mbhX[m];
                            mbhyVec[m] += mbhY[m];
                        }
                    }

                    ///allfeatures[288] = hog[96] + mbhX[96] + mbhY[96]
                    ///this.dimention = 32, this.nt_cell = 3
                    ///avg(trace[1-5]) -> nt_Cell[0], avg(trace[6-10]) -> ntCell[1], avg(trace[11-15]) -> ntCell[2]
                    int hogIndexSt = n * this.nt_cell;
                    int mbhxIndexSt = hogIndexSt + this.nt_cell * this.dimension;
                    int mbhyIndexSt = mbhxIndexSt + this.nt_cell * this.dimension;

                    for (int m = 0; m < this.dimension; m++) {
                        allFeatures[hogIndexSt + m] = hogVec[m] / (float) t_stride;
                        allFeatures[mbhxIndexSt + m] = mbhxVec[m] / (float) t_stride;
                        allFeatures[mbhyIndexSt + m] = mbhyVec[m] / (float) t_stride;
                    }
                }

//                if (allFeatures.length != 288) {
//                    throw new IllegalArgumentException("allFeatures.length != 288");
//                }
//                for (int i = 0; i < this.nt_cell * this.dimension; i++) {
//                    allFeatures[i] = hogFeatures[i];
//                }
//                for (int i = 0; i < this.nt_cell * this.dimension; i++) {
//                    allFeatures[this.nt_cell * this.dimension + i] = mbhXFeatures[i];
//                }
//                for (int i = 0; i < this.nt_cell * this.dimension; i++) {
//                    allFeatures[this.nt_cell * this.dimension * 2 + i] = mbhYFeatures[i];
//                }
                traceFeatures.add(allFeatures);
            }

            System.out.println("FeatureGenerate-finishedAdd: " + frameId + ", tCnt: " + traceRecords.size()
                    + ", traceFeature: " + traceFeatures.size() + "@" + System.currentTimeMillis());

            collector.emit(STREAM_FRAME_FV, tuple, new Values(frameId, traceFeatures));
            desMatMap.remove(frameId - this.maxTrackerLength - 10);
            traceData.remove(frameId);
        } else {
            System.out.println("finished: " + System.currentTimeMillis() + ":" + frameId
                    + ",desMatMap(" + desMatMap.containsKey(frameId) + ").Size: " + desMatMap.size()
                    + ",traceData(" + traceData.containsKey(frameId) + ").Size: " + traceData.size());
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        outputFieldsDeclarer.declareStream(STREAM_FRAME_FV, new Fields(FIELD_FRAME_ID, FIELD_FEA_VEC));
        outputFieldsDeclarer.declareStream(STREAM_CACHE_CLEAN, new Fields(FIELD_FRAME_ID));
    }
}

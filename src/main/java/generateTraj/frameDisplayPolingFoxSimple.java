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
    private int windowSize;

    int numDimension = 288;
    int numCluster = 256;
    int fvLength = 2 * numCluster * numDimension / 2;

    PcaData hogPca;
    PcaData mbhxPca;
    PcaData mbhyPca;

    GmmData hogGmm;
    GmmData mbhxGmm;
    GmmData mbhyGmm;

    List<float[]> trainingResult;

    private boolean toDebug = false;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rawFeatureDataList = new HashMap<>();
        fvCounter = new HashMap<>();
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
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        List<float[]> data = (List<float[]>) tuple.getValueByField(FIELD_FEA_VEC);

        int winIndex = (frameId - 1 - 14) / this.windowSize; ///there is an offset between frameID and Window
        if (rawFeatureDataList.containsKey(winIndex)) {
            rawFeatureDataList.get(winIndex).addAll(data);
            fvCounter.computeIfPresent(winIndex, (k, v) -> v + 1);
            System.out.println("frameID: " + frameId + ", winIndex: " + winIndex + ", fvCounter: " + fvCounter.get(winIndex));
            if (fvCounter.get(winIndex) == this.windowSize) {

                Object[] result = NewMethod.checkNew_float(rawFeatureDataList.get(winIndex), trainingResult,
                        numDimension, hogPca, mbhxPca, mbhyPca, hogGmm, mbhxGmm, mbhyGmm, true);
                int getClassificationID = (int)result[0];
                float sim = (float)result[1];

                rawFeatureDataList.remove(winIndex);
                fvCounter.remove(winIndex);
                System.out.println("simframeID: " + frameId + ", winIndex: " + winIndex + ", cResult: " + getClassificationID + ", sim: " + + sim +  ", ht.cnt: " + rawFeatureDataList.size());
            }
        } else {
            rawFeatureDataList.put(winIndex, data);
            fvCounter.put(winIndex, 1);
        }

        List<float[]> tmp = new ArrayList<>();
        collector.emit(STREAM_FRAME_FV, tuple, new Values(frameId, tmp));

//        collector.emit(STREAM_FRAME_FV, tuple, new Values(frameId, data));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_FV, new Fields(FIELD_FRAME_ID, FIELD_FEA_VEC));
    }
}

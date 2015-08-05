package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Parameters;
import logodetection.StormVideoLogoDetectorBeta;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;
import util.ConfigUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getListOfStrings;

/**
 * Created by Tom Fu at Mar 24, 2015
 * We try to enable to detector more than one logo template file
 */
public class tPatchProcessorDeltaNoLoop extends BaseRichBolt {
    OutputCollector collector;

    /**
     * Instance of detector
     */
    private List<StormVideoLogoDetectorBeta> detectors;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        int minNumberOfMatches = Math.min(getInt(map, "minNumberOfMatches"), 4);
        this.collector = outputCollector;
        // TODO: get path to logos & parameters from config
        Parameters parameters = new Parameters()
                .withMatchingParameters(
                        new Parameters.MatchingParameters()
                                .withMinimalNumberOfMatches(minNumberOfMatches)
                );

        List<String> templateFiles = getListOfStrings(map, "originalTemplateFileNames");
        int maxAdditionTemp = ConfigUtil.getInt(map, "maxAdditionTemp", 4);
        detectors = new ArrayList<>();
        for (int logoIndex = 0; logoIndex < templateFiles.size(); logoIndex ++) {
            detectors.add(new StormVideoLogoDetectorBeta(parameters, templateFiles.get(logoIndex), logoIndex, maxAdditionTemp));
        }
        System.out.println("tPatchProcessorDelta.prepare, with logos: " + detectors.size() + ", maxAdditionTemp: " + maxAdditionTemp);
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        //String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.PatchIdentifierMat identifierMat = (Serializable.PatchIdentifierMat) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);

        List<Serializable.Rect> detectedLogoList = new ArrayList<>();

        for (int logoIndex = 0; logoIndex < detectors.size(); logoIndex ++) {
            StormVideoLogoDetectorBeta detector = detectors.get(logoIndex);

            detector.detectLogosInMatRoi(identifierMat.sMat.toJavaCVMat(), identifierMat.identifier.roi.toJavaCVRect());
            Serializable.Rect detectedLogo = detector.getFoundRect();
            Serializable.Mat extractedTemplate = detector.getExtractedTemplate();

            detectedLogoList.add(detectedLogo);
        }
        //collector.emit(DETECTED_LOGO_STREAM, tuple, new Values(frameId, detectedLogo, patchCount));
        collector.emit(DETECTED_LOGO_STREAM, tuple, new Values(frameId, detectedLogoList, patchCount));

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DETECTED_LOGO_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FOUND_RECT, FIELD_PATCH_COUNT));
    }
}

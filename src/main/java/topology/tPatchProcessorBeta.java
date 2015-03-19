package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Debug;
import logodetection.Parameters;
import logodetection.StormVideoLogoDetector;
import org.bytedeco.javacpp.opencv_core;

import java.util.*;

import static topology.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getListOfStrings;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tPatchProcessorBeta extends BaseRichBolt {
    OutputCollector collector;

    /**
     * Instance of detector
     */
    private StormVideoLogoDetector detector;

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
        detector = new StormVideoLogoDetector(parameters, templateFiles);

        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(PATCH_FRAME_STREAM))
            processFrame(tuple);
        else if (streamId.equals(LOGO_TEMPLATE_UPDATE_STREAM))
            processNewTemplate(tuple);
        collector.ack(tuple);
    }

    //  Fields("frameId", "frameMat", "patchCount"));
    private void processFrame(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        List<Serializable.PatchIdentifierMat> identifierMatList = (List<Serializable.PatchIdentifierMat>) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);

        //List<Serializable.PatchIdentifierMat> updated

        for (int i = 0; i < identifierMatList.size(); i++) {
            Serializable.PatchIdentifierMat identifierMat = identifierMatList.get(i);
            detector.detectLogosInMatRoi(identifierMat.sMat.toJavaCVMat(), identifierMat.identifier.roi.toJavaCVRect());
            Serializable.Rect detectedLogo = detector.getFoundRect();
            Serializable.Mat extractedTemplate = detector.getExtractedTemplate();
            if (detectedLogo != null) {
                collector.emit(LOGO_TEMPLATE_UPDATE_STREAM, new Values(identifierMat.identifier, extractedTemplate, detector.getParentIdentifier()));
            }
            collector.emit(DETECTED_LOGO_STREAM, tuple,
                    new Values(frameId, identifierMat.identifier, detectedLogo, patchCount));
        }
    }

    // Fields("hostPatchIdentifier", "extractedTemplate", "parentIdentifier"));
    private void processNewTemplate(Tuple tuple) {
        Serializable.PatchIdentifier receivedPatchIdentifier = (Serializable.PatchIdentifier) tuple.getValueByField(FIELD_HOST_PATCH);
        Serializable.Mat extracted = (Serializable.Mat) tuple.getValueByField(FIELD_EXTRACTED_TEMPLATE);
        Serializable.PatchIdentifier parent = (Serializable.PatchIdentifier) tuple.getValueByField(FIELD_PARENT_PATCH);

        detector.addTemplateBySubMat(receivedPatchIdentifier, extracted, 5);
        detector.incrementPriority(parent, 1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DETECTED_LOGO_STREAM,
                new Fields(FIELD_FRAME_ID, FIELD_HOST_PATCH, FIELD_DETECTED_RECT, FIELD_PATCH_COUNT));

        //outputFieldsDeclarer.declareStream(LOGO_TEMPLATE_UPDATE_STREAM,
        //        new Fields(FIELD_HOST_PATCH, FIELD_EXTRACTED_TEMPLATE, FIELD_PARENT_PATCH));
    }
}

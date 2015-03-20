package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Parameters;
import logodetection.StormVideoLogoDetector;
import org.bytedeco.javacpp.opencv_core;

import java.util.List;
import java.util.Map;

import static topology.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getListOfStrings;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tPatchProcessorGamma extends BaseRichBolt {
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

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.PatchIdentifierMat patchIdentifierMat = (Serializable.PatchIdentifierMat) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);

        //detector.detectLogosInMatRoi(patchIdentifierMat.sMat.toJavaCVMat(), patchIdentifierMat.identifier.roi.toJavaCVRect());
        //Serializable.Rect detectedLogo = detector.getFoundRect();
        Serializable.Rect detectedLogo = null;
        //Serializable.Mat extractedTemplate = detector.getExtractedTemplate();

        collector.emit(DETECTED_LOGO_STREAM, tuple, new Values(frameId, detectedLogo));

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DETECTED_LOGO_STREAM, new Fields(FIELD_FRAME_ID, FIELD_DETECTED_RECT));
    }
}

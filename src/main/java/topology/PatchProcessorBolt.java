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
import tool.Serializable;

import java.util.*;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getListOfStrings;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class PatchProcessorBolt extends BaseRichBolt {
    OutputCollector collector;

    /** Instance of detector */
    private StormVideoLogoDetector detector;
    /** This counts from which patches the update has been already received */
    //private HashSet<Serializable.PatchIdentifier> receivedUpdatesFrom;
    //Modified by Tom on Sep 8, 2014
    private Map<Serializable.PatchIdentifier, Boolean> receivedUpdatesFrom;
    private static int MaxSizeOfReceivedUpdatesFrom = 4096;
    /** The receipt */
    private HashMap<Integer, Serializable.Mat> frameMap;

    private HashMap< Integer, Queue<Serializable.PatchIdentifier> > patchQueue;

    private HashMap< Integer, Queue<LogoTemplateUpdate> > templateQueue;


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
        //receivedUpdatesFrom = new HashSet<>();
        //Modified by Tom on Sep 8, 2014
        receivedUpdatesFrom = new LinkedHashMap<Serializable.PatchIdentifier, Boolean>(){
            @Override
            protected boolean removeEldestEntry(Map.Entry<Serializable.PatchIdentifier, Boolean> eldest) {
                return size() > MaxSizeOfReceivedUpdatesFrom;
            }
        };

        frameMap = new HashMap<>();
        patchQueue = new HashMap<>();
        templateQueue = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(RAW_FRAME_STREAM))
            processFrame(tuple);
        else if (streamId.equals(PATCH_STREAM))
            processPatch(tuple);
        else if (streamId.equals(LOGO_TEMPLATE_UPDATE_STREAM))
            processNewTemplate(tuple);
        else if (streamId.equals(CACHE_CLEAR_STREAM))
            processCacheClear(tuple);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DETECTED_LOGO_STREAM,
                new Fields(FIELD_FRAME_ID, FIELD_PATCH_IDENTIFIER, FIELD_FOUND_RECT, FIELD_PATCH_COUNT));

        outputFieldsDeclarer.declareStream(LOGO_TEMPLATE_UPDATE_STREAM,
                new Fields(FIELD_HOST_PATCH_IDENTIFIER, FIELD_DETECTED_LOGO_RECT, FIELD_PARENT_PATCH_IDENTIFIER));
    }

    //  Fields("frameId", "frameMat", "patchCount"));
    private void processFrame( Tuple tuple ) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat mat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);
        if (frameMap.containsKey(frameId)) {
            if (Debug.topologyDebugOutput)
                System.err.println(this.getClass() + "#" + "processFrame(): Received duplicate frame");
        } else {
            frameMap.put(frameId, mat);
        }
        if (patchQueue.containsKey(frameId)) {
            Queue<Serializable.PatchIdentifier> queue = patchQueue.get(frameId);
            while (!queue.isEmpty()) {
                Serializable.PatchIdentifier hostPatch = queue.poll();
                detector.detectLogosInRoi(mat.toJavaCVMat(), hostPatch.roi.toJavaCVRect());
                Serializable.Rect detectedLogo = detector.getFoundRect();
                if (detectedLogo != null) {// TODO: is anchoring really necessary?
                    collector.emit(LOGO_TEMPLATE_UPDATE_STREAM, tuple, new Values(hostPatch, detectedLogo, detector.getParentIdentifier()));
                }
                collector.emit(DETECTED_LOGO_STREAM, tuple,
                        new Values(frameId, hostPatch, detectedLogo, patchCount ));
            }
        } else {
            //patchQueue.put(frameId, new LinkedList<>());
        }
        if (templateQueue.containsKey(frameId)) {
            Queue<LogoTemplateUpdate> queue = templateQueue.get(frameId);
            while (!queue.isEmpty()) {
                LogoTemplateUpdate update = queue.poll();
                Serializable.Rect roi = update.detectedLogoRect;
                Serializable.PatchIdentifier hostPatchIdentifier = update.hostPatchIdentifier;
                Serializable.PatchIdentifier parent = update.parentIdentifier;
                detector.addTemplateByRect(hostPatchIdentifier, mat, roi);
                detector.incrementPriority(parent, 1);
            }
        } else {
            //templateQueue.put(frameId, new LinkedList<>());
        }

    }

    // Fields("patchIdentifier", "patchCount"));
    private void processPatch( Tuple tuple ) {
        Serializable.PatchIdentifier patchIdentifier = (Serializable.PatchIdentifier) tuple.getValueByField(FIELD_PATCH_IDENTIFIER);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);
        int frameId = patchIdentifier.frameId;
        if (frameMap.containsKey(frameId)) {
            detector.detectLogosInRoi(frameMap.get(frameId).toJavaCVMat(), patchIdentifier.roi.toJavaCVRect());
            Serializable.Rect detectedLogo = detector.getFoundRect();
            if (detectedLogo != null) {
                collector.emit(LOGO_TEMPLATE_UPDATE_STREAM, new Values(patchIdentifier, detectedLogo, detector.getParentIdentifier()));
            }
            collector.emit(DETECTED_LOGO_STREAM, tuple,
                    new Values(frameId, patchIdentifier, detectedLogo, patchCount));
        } else {
            if (!patchQueue.containsKey(frameId))
                patchQueue.put(frameId, new LinkedList<>());
            patchQueue.get(frameId).add(patchIdentifier);
        }
    }

    // Fields("hostPatchIdentifier", "detectedLogoRect", "parentIdentifier"));
    private void processNewTemplate(Tuple tuple) {
        Serializable.PatchIdentifier receivedPatchIdentifier = (Serializable.PatchIdentifier)tuple.getValueByField(FIELD_HOST_PATCH_IDENTIFIER);
        // TODO: This container could become very large, need to clear it after some time
        // Modified by Tom, use LinkedHashMap for receivedUpdatesFrom, it will automatically remove the oldest
        // element when its size beyond some threshold.
        if ( !receivedUpdatesFrom.containsKey(receivedPatchIdentifier) ) {
            receivedUpdatesFrom.put(receivedPatchIdentifier, Boolean.TRUE);
            Serializable.Rect roi = (Serializable.Rect) tuple.getValueByField(FIELD_DETECTED_LOGO_RECT);
            Serializable.PatchIdentifier parent = (Serializable.PatchIdentifier) tuple.getValueByField(FIELD_PARENT_PATCH_IDENTIFIER);
            int frameId = receivedPatchIdentifier.frameId;
            if (frameMap.containsKey(frameId)) {
                Serializable.Mat mat = frameMap.get(frameId);

                detector.addTemplateByRect(receivedPatchIdentifier, mat, roi);

                detector.incrementPriority(parent, 1);
            } else {
                if (!templateQueue.containsKey(frameId))
                    templateQueue.put(frameId, new LinkedList<>());
                templateQueue.get(frameId).add(new LogoTemplateUpdate(receivedPatchIdentifier, roi, parent));
            }

        } else {
            if (Debug.topologyDebugOutput)
                System.out.println("PatchProcessorBolt: Received duplicate message");
        }
    }

    // Fields("frameId")
    private void processCacheClear(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        frameMap.remove(frameId);
        patchQueue.remove(frameId);
        templateQueue.remove(frameId);
    }
}

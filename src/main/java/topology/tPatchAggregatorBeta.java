package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Debug;
import tool.Serializable;

import java.util.*;

import static tool.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class tPatchAggregatorBeta extends BaseRichBolt {
    OutputCollector collector;

    /* Keeps track on which patches of the certain frame have already been received */
    //Map< Integer, HashSet<Serializable.Rect> > frameAccount;
    Map< Integer, Integer> frameMonitor;

    /* Contains the list of logos found found on a given frame */
    Map< Integer, List<Serializable.Rect> > foundRectAccount;
    @Override

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        //frameAccount = new HashMap<>();
        frameMonitor = new HashMap<>();
        foundRectAccount = new HashMap<>();
    }

    //Fields("frameId", "framePatchIdentifier", "foundRect", "patchCount"));
    @Override
    public void execute(Tuple tuple) {

        //opencv_core.IplImage fk = new opencv_core.IplImage();

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        //Serializable.PatchIdentifier patchIdentifier = (Serializable.PatchIdentifier)tuple.getValueByField(FIELD_PATCH_IDENTIFIER);
        int patchCount              = tuple.getIntegerByField(FIELD_PATCH_COUNT);
        Serializable.Rect foundRect = (Serializable.Rect)tuple.getValueByField(FIELD_FOUND_RECT);

        /* Updating the list of detected logos on the frame */
        if (foundRect != null) {
            foundRectAccount.computeIfAbsent(frameId, k->new ArrayList<>()).add(foundRect);
        }
        frameMonitor.computeIfAbsent(frameId, k->0);
        frameMonitor.computeIfPresent(frameId, (k,v)->v+1);;

        /* If all patches of this frame are collected proceed to the frame aggregator */
        if (frameMonitor.get(frameId) == patchCount) {
            if (Debug.topologyDebugOutput)
                System.out.println("All parts of frame " + frameId + " received");
            collector.emit(PROCESSED_FRAME_STREAM, new Values(frameId, foundRectAccount.get(frameId)));
            //frameAccount.remove(frameId);
            frameMonitor.remove(frameId);
            foundRectAccount.remove(frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FOUND_RECT_LIST));
    }
}

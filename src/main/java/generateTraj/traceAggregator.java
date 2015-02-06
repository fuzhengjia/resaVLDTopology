package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.NullArgumentException;
import topology.Serializable;
import util.ConfigUtil;

import java.nio.FloatBuffer;
import java.util.*;
import java.util.concurrent.LinkedTransferQueue;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constant.*;
import static topology.Constants.CACHE_CLEAR_STREAM;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 */
public class traceAggregator extends BaseRichBolt {
    OutputCollector collector;

    private HashMap<Integer, HashSet<String>> traceMonitor;
    private HashMap<Integer, List<TraceRecord>> traceData;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        traceMonitor = new HashMap<>();
        traceData = new HashMap<>();
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        if (streamId.equals(STREAM_EXIST_TRACE) || streamId.equals(STREAM_RENEW_TRACE)) {
            ///Plot
            TraceRecord trace = (TraceRecord)tuple.getValueByField(FIELD_TRACE_RECORD);
            this.traceData.computeIfAbsent(frameId, k -> new ArrayList<TraceRecord>()).add(trace);

            if (this.traceMonitor.containsKey(frameId)){
                this.traceMonitor.get(frameId).remove(trace.traceID);
                //System.out.println("afterremove:, frameID: " + frameId + ", streamID: " + streamId + "," + this.traceMonitor.get(frameId).size());
                checkOuput(frameId);

            } else {
                throw new NullArgumentException("traceMonitor does not find entry, frameID: " + frameId);
            }

        } else if (streamId.equals(STREAM_REMOVE_TRACE)) {
            String traceID = tuple.getStringByField(FIELD_TRACE_IDENTIFIER);
            if (this.traceMonitor.containsKey(frameId)){
                this.traceMonitor.get(frameId).remove(traceID);
                //System.out.println("afterremove:, frameID: " + frameId + ", streamID: " + streamId + "," + this.traceMonitor.get(frameId).size());
                checkOuput(frameId);

            } else {
                throw new NullArgumentException("traceMonitor does not find entry, frameID: " + frameId);
            }

        } else if (streamId.equals(STREAM_REGISTER_TRACE)) {
            String traceID = tuple.getStringByField(FIELD_TRACE_IDENTIFIER);
            this.traceMonitor.computeIfAbsent(frameId, k -> new HashSet<String>()).add(traceID);
            //System.out.println("register tuple, frameID: " + frameId + ", streamID: " + streamId +"," + this.traceMonitor.get(frameId).size());
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        outputFieldsDeclarer.declareStream(STREAM_CACHE_CLEAN, new Fields(FIELD_FRAME_ID));
    }

    public void checkOuput(int frameID){
        if (this.traceMonitor.get(frameID).isEmpty()){
            ///we consider this frame is finished
            //System.out.println("AggregatorFinished, send out frameID: " + frameID);
            collector.emit(STREAM_PLOT_TRACE, new Values(frameID, this.traceData.get(frameID)));
            this.traceData.remove(frameID);
            this.traceMonitor.remove(frameID);
            collector.emit(STREAM_CACHE_CLEAN, new Values(frameID));
        }
    }

}

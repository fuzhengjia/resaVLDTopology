package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

import static tool.Constants.*;

/**
 * Created by ding on 14-7-3.
 */
public class FrameSource extends RedisQueueSpout {

    private int frameId;
    //private String idPrefix;

    public FrameSource(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        //idPrefix = String.format("s-%02d-", context.getThisTaskIndex() + 1);
    }

    @Override
    protected void emitData(Object data) {
        //String id = idPrefix + frameId++;
        String id = String.valueOf(frameId);
        collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, data), id);

        long nowTime = System.currentTimeMillis();
        System.out.printf("Sendout: " + nowTime + "," + frameId);
        frameId ++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES));
    }
}

package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Tom Fu on Jan 28, 2015
 * This version is an alternative implementation, which every time emit two consecutive raw frames,
 *  it ease the processing of imagePrepare and opticalFlow calculation, at the cost of more network transmissions
 */
public class FrameMatSourceOptFlow extends RedisQueueSpout {

    private int frameId;

    private byte[] sMatPrevBA;

    public FrameMatSourceOptFlow(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        sMatPrevBA = null;
    }

    @Override
    protected void emitData(Object data) {
        String id = String.valueOf(frameId);
        byte[] sMatBA = (byte[]) data;

        try {

            if (frameId > 0 && sMatPrevBA != null){
                collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, sMatBA, sMatPrevBA), id);

                long nowTime = System.currentTimeMillis();
                System.out.printf("Sendout: " + nowTime + "," + frameId);
            }
            frameId++;
            sMatPrevBA = java.util.Arrays.copyOf(sMatBA, sMatBA.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_FRAME_MAT_PREV));
    }
}

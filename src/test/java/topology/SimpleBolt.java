package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Intern04 on 12/8/2014.
 */
public class SimpleBolt extends BaseRichBolt {
    OutputCollector collector;
    int cache = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("loop-stream", new Fields("signal"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("loop-stream")) {
            System.err.println("Reading from loop-stream");
            int number = (int)input.getValueByField("signal");
            System.err.println("Received signal " + number);
            cache = 0;
            System.err.println("[Bolt " + Thread.currentThread().getId() + "]: Cache is zeroed:" + cache);

        } else {
            cache += (int)input.getValueByField("number");
            System.err.println("[Bolt]:" + Thread.currentThread().getId() +" Cache in now " + cache);
            if (cache > 100)
                collector.emit( "loop-stream", new Values(1) );
        }
    }
}

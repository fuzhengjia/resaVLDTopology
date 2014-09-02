package topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by Intern04 on 12/8/2014.
 */
public class TestLoopTopology {
    public static void main(String args[]) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SIMPLE_SPOUT", new SimpleSpout())
                .setNumTasks(2);

        builder.setBolt("SIMPLE_BOLT", new SimpleBolt(), 2)
                .shuffleGrouping("SIMPLE_SPOUT", "number-stream")
                .allGrouping("SIMPLE_BOLT", "loop-stream");

        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("first", new Config(), topology);
        Thread.sleep(200*1000);
        cluster.killTopology("first");
        cluster.shutdown();
    }
}

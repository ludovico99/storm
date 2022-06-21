package org.apache.storm;


import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;


public class TopologyBuilderTestIT implements Serializable {

    private final WordCount wordCount = new WordCount();

    @Test
    public void test_DistributedSum() {

        try (LocalCluster cluster = new LocalCluster()) {

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("spout", new RandomSentenceSpout(), 5);
            //shufflegrouping subscribes to the spout, and equally distributes
            //tuples (sentences) across instances of the SplitSentence bolt
            builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
            //fieldsgrouping subscribes to the split bolt, and
            //ensures that the same word is sent to the same instance (group by field 'word')
            builder.setBolt("count", this.wordCount , 12).fieldsGrouping("split", new Fields("word"));

            //new configuration
            Config conf = new Config();
            conf.setDebug(false);
            conf.setMaxTaskParallelism(3);

            String TOPOLOGY_NAME = "TOPOLOGY";
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
            Utils.sleep(30000);

            cluster.shutdown();

            Assert.assertEquals(27,WordCount.counts.size());


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Error in creating the cluster");
        }
    }
}

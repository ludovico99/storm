package org.apache.storm;

import org.apache.storm.topology.*;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;




import static org.mockito.Mockito.*;

public class BasicBoltExecutorIT {

    private final BasicBoltExecutor boltExecutor1 = spy(new BasicBoltExecutor(new BasicBolt()));
    private final BasicBoltExecutor boltExecutor2 = spy(new BasicBoltExecutor(new BasicBolt()));


    @Test
    public void testHandleTuple() {
        try (LocalCluster cluster = new LocalCluster()) {

            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("spout", new Spout(), 2);

            builder.setBolt("bolt-1",this.boltExecutor1 , 2).shuffleGrouping("spout");
            builder.setBolt("bolt-2",this.boltExecutor2 , 2).shuffleGrouping("bolt-2");

            //new configuration
            Config conf = new Config();
            conf.setDebug(false);
            conf.setMaxTaskParallelism(3);

            String TOPOLOGY_NAME = "TOPOLOGY";
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
            Utils.sleep(30000);

            cluster.shutdown();

           verify(this.boltExecutor1).execute(isA(Tuple.class));
           verify(this.boltExecutor2).execute(isA(Tuple.class));


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Error in creating the cluster");
        }
    }




}

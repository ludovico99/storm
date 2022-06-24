package org.apache.storm;

import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;


import java.lang.management.MemoryType;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

public class BasicBoltExecutorIT {


    @Test
    public void testHandleTuple() {
        try (LocalCluster cluster = new LocalCluster()) {

            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("spout", new Spout(), 5);

            builder.setBolt("bolt-1",  new BasicBoltExecutor(new MyBasicBolt()) , 6).fieldsGrouping("spout",new Fields("fields"));

            builder.setBolt("bolt-2",  new MyBasicBolt_2(), 7).fieldsGrouping("bolt-1",new Fields("fields"));

            //new configuration
            Config conf = new Config();
            conf.setDebug(false);
            conf.setMaxTaskParallelism(3);

            String TOPOLOGY_NAME = "TOPOLOGY-1";
            StormTopology stormTopology = builder.createTopology();

//            List<Object> spoutsObject = stormTopology.get_spouts().values()
//                    .stream()
//                    .map((spec) -> Thrift.deserializeComponentObject(spec.get_spout_object()))
//                    .collect(Collectors.toList());
//
//            Spout spout = spy((Spout) spoutsObject.get(0));
//
//            List<Object> boltsObject = stormTopology.get_bolts().values()
//                    .stream()
//                    .map((spec) -> Thrift.deserializeComponentObject(spec.get_bolt_object()))
//                    .collect(Collectors.toList());
//
//            BasicBoltExecutor bolt1 = spy((BasicBoltExecutor) boltsObject.get(0));
//            BasicBoltExecutor bolt2 = spy((BasicBoltExecutor) boltsObject.get(1));

            cluster.submitTopology(TOPOLOGY_NAME, conf, stormTopology);

            Utils.sleep(10000);

//            verify(spout).nextTuple();
//
//            ArgumentCaptor<Tuple> argument = ArgumentCaptor.forClass(Tuple.class);
//            verify(bolt1).execute(argument.capture());
//            Tuple tuple = argument.capture();
//            Assert.assertEquals(100, tuple.getValue(0));
//
//            verify(bolt2).execute(argument.capture());
//            tuple = argument.capture();
//            Assert.assertEquals(100, tuple.getValue(0));

            cluster.shutdown();

            Assert.assertEquals(Spout.toSend + MyBasicBolt.added, MyBasicBolt_2.received);


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Error in creating the cluster");
        }
    }




}

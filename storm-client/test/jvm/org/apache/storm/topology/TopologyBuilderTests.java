package org.apache.storm.topology;



import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.IWorkerHook;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;


@RunWith(value = Enclosed.class)
public class TopologyBuilderTests {

    @RunWith(Parameterized.class)
    public static class TestTopologyCreation {
        private TopologyBuilder topologyBuilder;
        private IBasicBolt basicBolt;
        private IRichBolt richBolt;
        private IStatefulBolt<?> statefulBolt;
        private IRichSpout richSpout;

        public TestTopologyCreation(ParamType spout, ParamType bolt) {
            configure(spout, bolt);
        }

        private void configure(ParamType spout, ParamType bolt) {
            this.topologyBuilder = new TopologyBuilder();


            switch (spout) {
                case VALID_INSTANCE:
                    this.richSpout = new BaseRichSpout() {
                        @Override
                        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

                        }

                        @Override
                        public void nextTuple() {

                        }

                        @Override
                        public void declareOutputFields(OutputFieldsDeclarer declarer) {

                        }
                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };
                    break;
                case INVALID_INSTANCE:
                    this.richSpout = new BaseRichSpout() {
                        @Override
                        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

                        }

                        @Override
                        public void nextTuple() {

                        }

                        @Override
                        public void declareOutputFields(OutputFieldsDeclarer declarer) {

                        }
                    };
                    break;
                case NULL_INSTANCE:
                    this.richSpout = null;
                    break;
            }

            switch (bolt) {
                case VALID_INSTANCE:
                    this.basicBolt = new BaseBasicBolt() {
                        @Override
                        public void execute(Tuple input, BasicOutputCollector collector) {

                        }

                        @Override
                        public void declareOutputFields(OutputFieldsDeclarer declarer) {

                        }
                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };

                    this.richBolt = new BaseRichBolt() {
                        @Override
                        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

                        }

                        @Override
                        public void execute(Tuple input) {

                        }

                        @Override
                        public void declareOutputFields(OutputFieldsDeclarer declarer) {

                        }
                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };
                    this.statefulBolt = new BaseStatefulBolt<State>() {
                        @Override
                        public void initState(State state) {

                        }

                        @Override
                        public void execute(Tuple input) {

                        }

                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };

                    break;
                case INVALID_INSTANCE:
                    this.basicBolt = new BaseBasicBolt() {
                        @Override
                        public void execute(Tuple input, BasicOutputCollector collector) {

                        }

                        @Override
                        public void declareOutputFields(OutputFieldsDeclarer declarer) {

                        }
                    };

                    this.richBolt = new BaseRichBolt() {
                        @Override
                        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

                        }

                        @Override
                        public void execute(Tuple input) {

                        }

                        @Override
                        public void declareOutputFields(OutputFieldsDeclarer declarer) {

                        }
                    };
                    this.statefulBolt = new BaseStatefulBolt<State>() {
                        @Override
                        public void initState(State state) {

                        }
                        @Override
                        public void execute(Tuple input) {

                        }
                    };


                    break;
                case NULL_INSTANCE:
                    this.basicBolt = null;
                    this.richBolt = null;
                    this.statefulBolt = null;
                    break;
            }


            this.topologyBuilder.setSpout("spout1", this.richSpout);


            this.topologyBuilder.setBolt("bolt1", this.statefulBolt, 1)
                    .shuffleGrouping("spout1");
            this.topologyBuilder.setBolt("bolt2", this.richBolt, 1)
                    .shuffleGrouping("bolt1");
            this.topologyBuilder.setBolt("bolt3", this.basicBolt, 1)
                    .shuffleGrouping("bolt2");


        }


        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    //SPOUT                   BOLT
                    {ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE}
            });
        }


        @Test
        public void test_createTopology() {

            StormTopology topology = topologyBuilder.createTopology();

            Assert.assertNotNull(topology);

            Set<String> spouts = topology.get_spouts().keySet();
            Assert.assertEquals(ImmutableSet.of("spout1", "$checkpointspout"), spouts);

            Assert.assertEquals(ImmutableSet.of(new GlobalStreamId("spout1", "default"),
                            new GlobalStreamId("$checkpointspout", "$checkpoint")),
                    topology.get_bolts().get("bolt1").get_common().get_inputs().keySet());


            Assert.assertEquals(ImmutableSet.of(new GlobalStreamId("bolt1", "default"),
                            new GlobalStreamId("bolt1", "$checkpoint")),
                    topology.get_bolts().get("bolt2").get_common().get_inputs().keySet());


            Assert.assertEquals(ImmutableSet.of(new GlobalStreamId("bolt2", "default"),
                            new GlobalStreamId("bolt2", "$checkpoint")),
                    topology.get_bolts().get("bolt3").get_common().get_inputs().keySet());

        }
    }

    @RunWith(Parameterized.class)
    public static class TestSetBolt{

        private TopologyBuilder topologyBuilder;
        private IBasicBolt basicBolt;
        private IRichBolt richBolt;
        private IStatefulBolt<?> statefulBolt;
        private IRichSpout richSpout;
        private IWorkerHook workerHook;
        private Number parallelismHint;

        private boolean expectedValueBolts;
        private boolean expectedValueSpout;
        private boolean expectedWorkerHook;

        public TestSetBolt(ParamType spout, ParamType bolt, ParamType parallelismHint, ParamType workerHook) {
            configure(spout, bolt, parallelismHint,workerHook);
        }

        private void configure(ParamType spout, ParamType bolt, ParamType parallelismHint,  ParamType workerHook) {
            this.topologyBuilder = new TopologyBuilder();


            switch (spout) {
                case VALID_INSTANCE:
                    this.richSpout = mock(IRichSpout.class);

                    this.expectedValueSpout = false;
                    break;
                case INVALID_INSTANCE:
                    break;
                case NULL_INSTANCE:
                    this.richSpout = null;

                    this.expectedValueSpout = true;
                    break;
            }


            switch (bolt) {
                case VALID_INSTANCE:
                    this.basicBolt = mock(IBasicBolt.class);
                    this.richBolt = mock(IRichBolt.class);
                    this.statefulBolt = mock(IStatefulBolt.class);

                    this.expectedValueBolts = false;
                    break;
                case INVALID_INSTANCE:
                    break;
                case NULL_INSTANCE:
                    this.basicBolt = null;
                    this.richBolt = null;
                    this.statefulBolt = null;

                    this.expectedValueBolts = true;
                    break;
            }

            switch (parallelismHint) {
                case VALID_INSTANCE:
                    this.parallelismHint = 1;
                    break;
                case INVALID_INSTANCE:
                    this.parallelismHint = -1;
                    this.expectedValueBolts = true;
                    this.expectedValueSpout = true;
                    break;
                case NULL_INSTANCE:
                    this.parallelismHint = null;
                    this.expectedValueBolts = true;
                    this.expectedValueSpout = true;
                    break;
            }

            switch (workerHook){
                case VALID_INSTANCE:
                    this.workerHook = new IWorkerHook()  {
                        @Override
                        public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {

                        }

                        @Override
                        public void shutdown() {

                        }

                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };
                    break;
                case INVALID_INSTANCE:
                    this.workerHook = new IWorkerHook() {
                        @Override
                        public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {

                        }

                        @Override
                        public void shutdown() {

                        }
                    };
                    this.expectedWorkerHook = true;
                    break;

                case NULL_INSTANCE:
                    this.workerHook = null;
                    this.expectedWorkerHook = true;
                    break;

            }


        }


        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    //SPOUT,                   BOLT,                   , PaparallelismHint >0     ,IWorkerHook
                    {ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE},
                    {ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE},
                    {ParamType.NULL_INSTANCE,  ParamType.NULL_INSTANCE,  ParamType.NULL_INSTANCE,    ParamType.NULL_INSTANCE}
            });
        }

        @Test
        public void testSetRichBolt() {
            try {
                topologyBuilder.setBolt("bolt", this.richBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            }catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }

        }

        @Test
        public void testSetBasicBolt() {
            try {
                topologyBuilder.setBolt("bolt", this.basicBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            }catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }

        }

        @Test
        public void testSetStatefulBolt() {
            try {
               topologyBuilder.setBolt("bolt", this.statefulBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            }catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetSpout() {
            try {
                topologyBuilder.setSpout("bolt", this.richSpout, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueSpout);
            }catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueSpout);
            }
        }

        @Test
        public void testAddWorkerHook() {
            try {
                topologyBuilder.addWorkerHook(this.workerHook);
                Assert.assertFalse("No exception was expected", this.expectedWorkerHook);
            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", this.expectedWorkerHook);
            }
        }
    }


}

package org.apache.storm.topology;



import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.IWorkerHook;
import org.apache.storm.lambda.SerializableSupplier;
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

import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(value = Enclosed.class)
public class TopologyBuilderTests {

    @RunWith(Parameterized.class)
    public static class TestTopologyCreation {
        private TopologyBuilder topologyBuilder;
        private IBasicBolt basicBolt;
        private IRichBolt richBolt;
        private IStatefulBolt<?> statefulBolt;
        private IRichSpout richSpout;
        private boolean expectedException = false;
        private Object expectedSpoutsSet;
        private List<ImmutableSet<GlobalStreamId>> expectedInputForBolts;


        private final String[] bolts = {"bolt1", "bolt2", "bolt3"};
        private final String spout1 = "spout1";

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

                        private void writeObject(java.io.ObjectOutputStream stream) {
                        }
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

                    this.expectedException = true;

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

                        private void writeObject(java.io.ObjectOutputStream stream) {
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

                        private void writeObject(java.io.ObjectOutputStream stream) {
                        }
                    };
                    this.statefulBolt = new BaseStatefulBolt<State>() {
                        @Override
                        public void initState(State state) {

                        }

                        @Override
                        public void execute(Tuple input) {

                        }

                        private void writeObject(java.io.ObjectOutputStream stream) {
                        }
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
                    this.expectedException = true;
                    break;
            }


            this.topologyBuilder.setSpout(this.spout1, this.richSpout);

            this.topologyBuilder.setBolt(this.bolts[0], this.statefulBolt, 1)
                    .shuffleGrouping(this.spout1);
            this.topologyBuilder.setBolt(this.bolts[1], this.richBolt, 1)
                    .shuffleGrouping(this.bolts[0]);
            this.topologyBuilder.setBolt(this.bolts[2], this.basicBolt, 1)
                    .shuffleGrouping(this.bolts[1]);

            this.expectedSpoutsSet = ImmutableSet.of("spout1", "$checkpointspout");

            this.expectedInputForBolts = new ArrayList<>();
            this.expectedInputForBolts.add(ImmutableSet.of(new GlobalStreamId(this.spout1, "default"),
                    new GlobalStreamId("$checkpointspout", "$checkpoint")));
            this.expectedInputForBolts.add(ImmutableSet.of(new GlobalStreamId(this.bolts[0], "default"),
                    new GlobalStreamId(this.bolts[0], "$checkpoint")));
            this.expectedInputForBolts.add(ImmutableSet.of(new GlobalStreamId(this.bolts[1], "default"),
                    new GlobalStreamId(this.bolts[1], "$checkpoint")));

        }


        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    //SPOUT                   BOLT
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE},
                    {ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE},
                    {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE},
                    {ParamType.VALID_INSTANCE,   ParamType.INVALID_INSTANCE}

            });
        }


        @Test
        public void test_createTopology() {
            try {

                StormTopology topology = topologyBuilder.createTopology();

                Assert.assertNotNull(topology);

                Set<String> spouts = topology.get_spouts().keySet();
                Assert.assertEquals(this.expectedSpoutsSet, spouts);

                Assert.assertEquals(this.expectedInputForBolts.get(0), topology.get_bolts().get(this.bolts[0]).get_common().get_inputs().keySet());

                Assert.assertEquals(this.expectedInputForBolts.get(1), topology.get_bolts().get(this.bolts[1]).get_common().get_inputs().keySet());

                Assert.assertEquals(this.expectedInputForBolts.get(2), topology.get_bolts().get(this.bolts[2]).get_common().get_inputs().keySet());

            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", this.expectedException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class TestSetBolt {

        private TopologyBuilder topologyBuilder;

        private IBasicBolt basicBolt;
        private IRichBolt richBolt;
        private IStatefulBolt<?> statefulBolt;
        private IStatefulWindowedBolt<?> statefulWindowedBolt;
        private IStatefulWindowedBolt<?> statefulWindowedBoltPersistent;

        private IRichSpout richSpout;
        private SerializableSupplier<?> supplier;

        private IWorkerHook workerHook;
        private Number parallelismHint;

        private String[] boltsId;
        private String[] spoutsId;

        private boolean expectedValueBolts;
        private boolean expectedValueSpout;
        private boolean expectedWorkerHook;
        private boolean expectedSupplier;

        public TestSetBolt(ParamType spout, ParamType bolt, ParamType parallelismHint, ParamType workerHook,
                           ParamType serializableSupp) {
            configure(spout, bolt, parallelismHint, workerHook, serializableSupp);
        }

        private void configure(ParamType spout, ParamType bolt, ParamType parallelismHint, ParamType workerHook, ParamType serializableSupp) {
            this.topologyBuilder = new TopologyBuilder();
            this.basicBolt = mock(IBasicBolt.class);
            this.richBolt = mock(IRichBolt.class);
            this.statefulBolt = mock(IStatefulBolt.class);
            this.statefulWindowedBolt = mock(IStatefulWindowedBolt.class);
            when(statefulWindowedBolt.isPersistent()).thenReturn(false);

            this.statefulWindowedBoltPersistent = mock(IStatefulWindowedBolt.class);
            when(statefulWindowedBoltPersistent.isPersistent()).thenReturn(true);

            this.richSpout = mock(IRichSpout.class);


            switch (spout) {
                case VALID_INSTANCE:
                    this.spoutsId = new String[]{"spout1", "spout2"};
                    this.expectedValueSpout = false;
                    break;
                case INVALID_INSTANCE:
                    this.spoutsId = new String[]{"spout", "spout"};
                    this.expectedValueSpout = true;
                    break;
                case NULL_INSTANCE:
                    this.richSpout = null;
                    this.expectedValueSpout = true;
                    break;
            }


            switch (bolt) {
                case VALID_INSTANCE:
                    this.boltsId = new String[]{"bolt1", "bolt2"};
                    this.expectedValueBolts = false;

                    break;

                case INVALID_INSTANCE:
                    this.boltsId = new String[]{"bolt", "bolt"};
                    this.expectedValueBolts = true;
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

            switch (workerHook) {
                case VALID_INSTANCE:
                    this.workerHook = new IWorkerHook() {
                        @Override
                        public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {

                        }

                        @Override
                        public void shutdown() {

                        }

                        private void writeObject(java.io.ObjectOutputStream stream) {
                        }
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

            switch (serializableSupp){
                case VALID_INSTANCE:
                    this.supplier = new SerializableSupplier<Object>() {
                        @Override
                        public Object get() {
                            return null;
                        }
                        private void writeObject(java.io.ObjectOutputStream stream) {
                        }
                    };
                    this.expectedSupplier = false;
                    break;
                case INVALID_INSTANCE:
                    this.supplier = new SerializableSupplier<Object>() {
                        @Override
                        public Object get() {
                            return null;
                        }
                    };
                    this.expectedSupplier = true;
                    break;
                case NULL_INSTANCE:
                    this.supplier = null;
                    this.expectedSupplier = true;
                    break;
            }
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    //SPOUT,                   BOLT,                   , PaparallelismHint >0         ,IWorkerHook
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE},
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE},
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE},
                    {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE},
                    {ParamType.VALID_INSTANCE,   ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE},
                    {ParamType.NULL_INSTANCE,    ParamType.NULL_INSTANCE,   ParamType.VALID_INSTANCE,  ParamType.NULL_INSTANCE, ParamType.NULL_INSTANCE}
            });
        }

        @Test
        public void testSetRichBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.richBolt, this.parallelismHint);
                topologyBuilder.setBolt(this.boltsId[1], this.richBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }

        }

        @Test
        public void testSetBasicBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.basicBolt, this.parallelismHint);
                topologyBuilder.setBolt(this.boltsId[1], this.basicBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }

        }

        @Test
        public void testSetStatefulBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.statefulBolt, this.parallelismHint);
                topologyBuilder.setBolt(this.boltsId[1], this.statefulBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetStatefulWindowedBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.statefulWindowedBolt, this.parallelismHint);
                topologyBuilder.setBolt(this.boltsId[1], this.statefulWindowedBolt, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetStatefulWindowedBoltPersistent() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.statefulWindowedBoltPersistent, this.parallelismHint);
                topologyBuilder.setBolt(this.boltsId[1], this.statefulWindowedBoltPersistent, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetSpout() {
            try {
                topologyBuilder.setSpout(this.spoutsId[0], this.richSpout, this.parallelismHint);
                topologyBuilder.setSpout(this.spoutsId[1], this.richSpout, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueSpout);

            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueSpout);
            }
        }

        @Test
        public void testSetSpoutSupplier() {
            try {
                topologyBuilder.setSpout(this.spoutsId[0], this.supplier, this.parallelismHint);
                topologyBuilder.setSpout(this.spoutsId[1], this.supplier, this.parallelismHint);
                Assert.assertFalse("No exception was expected", this.expectedValueSpout);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueSpout);
            }
        }


        @Test
        public void testAddWorkerHook() {
            try {
                topologyBuilder.addWorkerHook(this.workerHook);
                Assert.assertFalse("No exception was expected", this.expectedWorkerHook);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", this.expectedWorkerHook);
            }
        }
    }

}

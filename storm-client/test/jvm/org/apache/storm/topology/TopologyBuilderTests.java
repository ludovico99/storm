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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_COMPONENT_ID;
import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_STREAM_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(value = Enclosed.class)
public class TopologyBuilderTests {

    @RunWith(Parameterized.class)
    public static class TestTopologyCreation {
        private TopologyBuilder topologyBuilder;
        private ParamType boltParamType;
        private ParamType spoutParamType;
        private boolean expectedException = false;
        private Object expectedSpoutsSet;
        private List<ImmutableSet<GlobalStreamId>> expectedInputForBolts;


        private final String[] bolts = {"bolt1", "bolt2", "bolt3"};
        private final String spout = "spout1";

        public TestTopologyCreation(ParamType spout, ParamType bolt, TopologyConfig topologyConfig) {
            configure(spout, bolt, topologyConfig);
        }

        private void configure(ParamType spout, ParamType bolt, TopologyConfig topologyConfig) {
            this.topologyBuilder = new TopologyBuilder();
            this.boltParamType = bolt;
            this.spoutParamType = spout;

            switch (spout) {
                case VALID_INSTANCE:
                    break;
                case INVALID_INSTANCE:
                    this.expectedException = true;
                    break;
            }

            switch (bolt) {
                case VALID_INSTANCE:
                    break;
                case INVALID_INSTANCE:
                    this.expectedException = true;
                    break;
            }

            switch (topologyConfig){
                case STATEFUL_BOLT:
                    this.topologyBuilder.setSpout(this.spout, richSpout());

                    this.topologyBuilder.setBolt(this.bolts[0], richBolt(), 1)
                            .shuffleGrouping(this.spout);
                    this.topologyBuilder.setBolt(this.bolts[1], statefulBolt(), 1)
                            .shuffleGrouping(this.bolts[0]);
                    this.topologyBuilder.setBolt(this.bolts[2], basicBolt(), 1)
                            .shuffleGrouping(this.bolts[1]).shuffleGrouping(this.spout);

                    this.expectedSpoutsSet = ImmutableSet.of("spout1", "$checkpointspout");


                    this.expectedInputForBolts = new ArrayList<>();

                    this.expectedInputForBolts.add(ImmutableSet.of(
                            new GlobalStreamId(this.spout, "default"),
                            new GlobalStreamId(CHECKPOINT_COMPONENT_ID, CHECKPOINT_STREAM_ID)));
                    this.expectedInputForBolts.add(ImmutableSet.of(
                            new GlobalStreamId(this.bolts[0], "default"),
                            new GlobalStreamId(this.bolts[0],CHECKPOINT_STREAM_ID)));
                    this.expectedInputForBolts.add(ImmutableSet.of(
                            new GlobalStreamId(this.bolts[1], "default"),
                            new GlobalStreamId(this.bolts[1], CHECKPOINT_STREAM_ID),
                            new GlobalStreamId(this.spout, "default"),
                            new GlobalStreamId(CHECKPOINT_COMPONENT_ID, CHECKPOINT_STREAM_ID)));


                    break;
                case NO_STATEFUL_BOLT:
                    this.topologyBuilder.setSpout(this.spout, richSpout());

                    this.topologyBuilder.setBolt(this.bolts[0], richBolt(), 1)
                            .shuffleGrouping(this.spout);
                    this.topologyBuilder.setBolt(this.bolts[1], richBolt(), 1)
                            .shuffleGrouping(this.bolts[0]);
                    this.topologyBuilder.setBolt(this.bolts[2], basicBolt(), 1)
                            .shuffleGrouping(this.bolts[1]).shuffleGrouping(this.spout);

                    this.expectedSpoutsSet = ImmutableSet.of(this.spout);


                    this.expectedInputForBolts = new ArrayList<>();
                    this.expectedInputForBolts.add(ImmutableSet.of(new GlobalStreamId(this.spout, "default")));
                    this.expectedInputForBolts.add(ImmutableSet.of(new GlobalStreamId(this.bolts[0], "default")));
                    this.expectedInputForBolts.add(ImmutableSet.of(
                            new GlobalStreamId(this.bolts[1], "default"),
                            new GlobalStreamId(this.spout,"default")));

                    break;

            }

        }


        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    //SPOUT                      BOLT
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE, TopologyConfig.STATEFUL_BOLT},
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE, TopologyConfig.NO_STATEFUL_BOLT},
                    {ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE, TopologyConfig.STATEFUL_BOLT},
                    {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, TopologyConfig.STATEFUL_BOLT},
                    {ParamType.VALID_INSTANCE,   ParamType.INVALID_INSTANCE, TopologyConfig.STATEFUL_BOLT}

            });
        }

        private IRichBolt richBolt() {
            if (this.boltParamType.equals(ParamType.VALID_INSTANCE)) return new BaseRichBolt() {
                @Override
                public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

                @Override
                public void execute(Tuple input) {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}

            };
            else {
                return new BaseRichBolt() {
                    @Override
                    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

                    @Override
                    public void execute(Tuple input) {}

                    @Override
                    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                };
            }
        }

        private IBasicBolt basicBolt() {
            if (this.boltParamType.equals(ParamType.VALID_INSTANCE)) return new BaseBasicBolt() {
                @Override
                public void execute(Tuple input, BasicOutputCollector collector) {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
            else {
                return new BaseBasicBolt() {
                    @Override
                    public void execute(Tuple input, BasicOutputCollector collector) {}

                    @Override
                    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
                };
            }
        }

        private IStatefulBolt<?> statefulBolt() {
            if (this.boltParamType.equals(ParamType.VALID_INSTANCE)) return new BaseStatefulBolt<State>() {
                @Override
                public void initState(State state) {}

                @Override
                public void execute(Tuple input) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
            else {
                return new BaseStatefulBolt<State>() {
                    @Override
                    public void initState(State state) {}

                    @Override
                    public void execute(Tuple input) {}
                };
            }
        }

        private IRichSpout richSpout() {
            if (this.spoutParamType.equals(ParamType.VALID_INSTANCE)) return new BaseRichSpout() {
                @Override
                public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

                @Override
                public void nextTuple() {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
            else {
                return new BaseRichSpout() {
                    @Override
                    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

                    @Override
                    public void nextTuple() {}

                    @Override
                    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
                };

            }
        }


        @Test
        public void test_createTopology() {
            try {

                StormTopology topology = this.topologyBuilder.createTopology();

                Assert.assertNotNull(topology);

                Set<String> spouts = topology.get_spouts().keySet();
                Assert.assertEquals(this.expectedSpoutsSet, spouts);

                Assert.assertEquals(this.expectedInputForBolts.get(0), topology.get_bolts().get(this.bolts[0]).get_common().get_inputs().keySet());

                Assert.assertEquals(this.expectedInputForBolts.get(1), topology.get_bolts().get(this.bolts[1]).get_common().get_inputs().keySet());

                Assert.assertEquals(this.expectedInputForBolts.get(2), topology.get_bolts().get(this.bolts[2]).get_common().get_inputs().keySet());

                Assert.assertFalse("No exception wa expected", this.expectedException);

            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception was expected", this.expectedException);
            }
        }

    }


    @RunWith(Parameterized.class)
    public static class TestSetBolt {

        private TopologyBuilder topologyBuilder;
        private String[] boltsId;
        private String[] spoutsId;

        private IBasicBolt basicBolt;
        private IRichBolt richBolt;
        private IStatefulBolt<?> statefulBolt;
        private IStatefulWindowedBolt<?> statefulWindowedBolt;
        private IStatefulWindowedBolt<?> statefulWindowedBoltPersistent;
        private Number parallelismHintBolts;
        private boolean expectedValueBolts;
        private IWindowedBolt windowedBolt;

        private IRichSpout richSpout;
        private Number parallelismHintSpout;
        private boolean expectedValueSpout;

        private SerializableSupplier<?> supplier;
        private Number parallelismHintSpoutSupplier;
        private boolean expectedSupplier;

        private IWorkerHook workerHook;
        private boolean expectedWorkerHook;

        private String[] boltsIdNP;
        private IBasicBolt basicBoltNP;
        private IRichBolt richBoltNP;
        private IStatefulBolt<?> statefulBoltNP;
        private IStatefulWindowedBolt<?> statefulWindowedBoltNP;
        private IStatefulWindowedBolt<?> statefulWindowedBoltPersistentNP;
        private boolean expectedValueBoltsNP;
        private IWindowedBolt windowedBoltNP;


        public TestSetBolt(Object[] setSpoutTest,Object[] setBoltTest, Object[] addWorkerHookTest,Object[] setSpoutSupplier
                , Object[] noParallelism) {

            configureSetBolt((ParamType) setBoltTest[0], (ParamType) setBoltTest[1], (Boolean) setBoltTest[2]);

            configureSetSpout((ParamType) setSpoutTest[0], (ParamType) setSpoutTest[1], (Boolean) setSpoutTest[2]);

            configureSetSpoutSupplier((ParamType) setSpoutSupplier[0], (ParamType) setSpoutSupplier[1], (Boolean) setSpoutSupplier[2]);

            configureAddWorkerHook((ParamType) addWorkerHookTest[0], (Boolean) addWorkerHookTest[1]);

            configureNoParallelism((ParamType) noParallelism[0], (Boolean) noParallelism[1]);

        }

        private void configureNoParallelism(ParamType bolt, Boolean expectedValueBoltsNP) {
            this.expectedValueBoltsNP = expectedValueBoltsNP;

            switch (bolt) {
                case VALID_INSTANCE:
                case INVALID_INSTANCE:
                    this.basicBoltNP = mock(IBasicBolt.class);
                    this.richBoltNP = mock(IRichBolt.class);
                    this.statefulBoltNP = mock(IStatefulBolt.class);
                    this.windowedBoltNP=mock(IWindowedBolt.class);
                    this.statefulWindowedBoltNP = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltNP.isPersistent()).thenReturn(false);
                    this.statefulWindowedBoltPersistentNP = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltPersistentNP.isPersistent()).thenReturn(true);
                    break;

                case NULL_INSTANCE:
                    this.basicBoltNP = null;
                    this.richBoltNP = null;
                    this.statefulBoltNP = null;
                    this.statefulWindowedBoltNP = null;
                    this.statefulWindowedBoltPersistentNP = null;
                    break;
            }

            if(bolt.equals(ParamType.VALID_INSTANCE))this.boltsIdNP = new String[]{"bolt1", "bolt2"};
            else this.boltsIdNP = new String[]{"bolt", "bolt"};

        }

        private void configureAddWorkerHook(ParamType workerHook, Boolean expectedWorkerHook) {
            this.expectedWorkerHook = expectedWorkerHook;

            switch (workerHook) {
                case VALID_INSTANCE:
                    this.workerHook = new IWorkerHook() {
                        @Override
                        public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {}

                        @Override
                        public void shutdown() {}

                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };
                    break;
                case INVALID_INSTANCE:
                    this.workerHook = new IWorkerHook() {
                        @Override
                        public void start(Map<String, Object> topoConf, WorkerTopologyContext context) {}

                        @Override
                        public void shutdown() {}
                    };
                    break;

                case NULL_INSTANCE:
                    this.workerHook = null;
                    break;
            }


        }

        private void configureSetSpoutSupplier(ParamType supplier, ParamType parallelismHint, Boolean expectedSupplier) {

            this.expectedSupplier = expectedSupplier;

            switch (supplier){
                case VALID_INSTANCE:
                    this.supplier = new SerializableSupplier<Object>() {
                        @Override
                        public Object get() {
                            return null;
                        }

                        private void writeObject(java.io.ObjectOutputStream stream) {}
                    };
                    break;
                case INVALID_INSTANCE:
                    this.supplier = (SerializableSupplier<Object>) () -> null;
                    break;
                case NULL_INSTANCE:
                    this.supplier = null;
                    break;
            }

            switch (parallelismHint) {
                case VALID_INSTANCE:
                    this.parallelismHintSpoutSupplier = 1;
                    break;
                case INVALID_INSTANCE:
                    this.parallelismHintSpoutSupplier = -1;

                    break;
                case NULL_INSTANCE:
                    this.parallelismHintSpoutSupplier = null;
                    break;
            }

        }

        private void configureSetSpout(ParamType spout, ParamType parallelismHint, Boolean expectedValueSpout) {
            this.richSpout = mock(IRichSpout.class);
            this.expectedValueSpout = expectedValueSpout;

            switch (spout) {
                case VALID_INSTANCE:
                    this.spoutsId = new String[]{"spout1", "spout2"};
                    break;
                case INVALID_INSTANCE:
                    this.spoutsId = new String[]{"spout", "spout"};
                    break;
                case NULL_INSTANCE:
                    this.richSpout = null;
                    break;
            }

            switch (parallelismHint) {
                case VALID_INSTANCE:
                    this.parallelismHintSpout = 1;
                    break;
                case INVALID_INSTANCE:
                    this.parallelismHintSpout = -1;
                    break;
                case NULL_INSTANCE:
                    this.parallelismHintSpout = null;
                    break;
            }
        }

        private void configureSetBolt(ParamType bolt, ParamType parallelismHint, boolean expectedValueBolts) {

            this.expectedValueBolts = expectedValueBolts;

            switch (bolt) {
                case VALID_INSTANCE:
                case INVALID_INSTANCE:
                    this.boltsId = new String[]{"bolt1", "bolt2"};
                    this.basicBolt = mock(IBasicBolt.class);
                    this.richBolt = mock(IRichBolt.class);
                    this.statefulBolt = mock(IStatefulBolt.class);
                    this.windowedBolt = mock(IWindowedBolt.class);
                    this.statefulWindowedBolt = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBolt.isPersistent()).thenReturn(false);

                    this.statefulWindowedBoltPersistent = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltPersistent.isPersistent()).thenReturn(true);
                    break;

                case NULL_INSTANCE:
                    this.basicBolt = null;
                    this.richBolt = null;
                    this.statefulBolt = null;
                    this.statefulWindowedBolt = null;
                    this.statefulWindowedBoltPersistent = null;
                    break;
            }

            switch (parallelismHint) {
                case VALID_INSTANCE:
                    this.parallelismHintBolts = 1;
                    break;
                case INVALID_INSTANCE:
                    this.parallelismHintBolts = -1;
                    break;
                case NULL_INSTANCE:
                    this.parallelismHintBolts = null;
                    break;
            }

            if(bolt.equals(ParamType.VALID_INSTANCE))this.boltsId = new String[]{"bolt1", "bolt2"};
            else this.boltsId = new String[]{"bolt", "bolt"};
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                                   //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] { ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, false},
                                         //BOLT,                    PaparallelismHint >0
                            new Object[] {ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, false},
                            //IWorkerHook
                            new Object[] {ParamType.VALID_INSTANCE, false},
                                         //Serializable supplier,      , PaparallelismHint >0
                            new Object[] { ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, false},
                                        //BOLT
                            new Object[] {ParamType.VALID_INSTANCE, false}
                    },

                                    //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] { ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},
                            //BOLT,                    PaparallelismHint >0
                            new Object[] {ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, true},
                            //IWorkerHook
                            new Object[] {ParamType.INVALID_INSTANCE, true},
                                         //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},
                                          //BOLT
                            new Object[] {ParamType.INVALID_INSTANCE, true}
                    },

                                    //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] { ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, true},
                                        //BOLT,                    PaparallelismHint >0
                            new Object[] {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},
                            //IWorkerHook
                            new Object[] {ParamType.VALID_INSTANCE, false},
                            //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, true},
                                         //BOLT
                            new Object[] {ParamType.INVALID_INSTANCE, true}
                    },

                                  //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] { ParamType.VALID_INSTANCE, ParamType.NULL_INSTANCE, false},
                            //BOLT,                    PaparallelismHint >0
                            new Object[] {ParamType.NULL_INSTANCE, ParamType.VALID_INSTANCE, true},
                            //IWorkerHook
                            new Object[] {ParamType.NULL_INSTANCE, true},
                            //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {ParamType.VALID_INSTANCE, ParamType.NULL_INSTANCE, false},
                                        //BOLT
                            new Object[] {ParamType.INVALID_INSTANCE, true}
                    },
            });
        }

        @Before
        public void set_up(){
            this.topologyBuilder = new TopologyBuilder();
        }


        @Test
        public void testSetRichBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.richBolt, this.parallelismHintBolts);
                topologyBuilder.setBolt(this.boltsId[1], this.richBolt, this.parallelismHintBolts);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }

        }

        @Test
        public void testSetRichBoltNoParallelismHint() {
            try {
                topologyBuilder.setBolt(this.boltsIdNP[0], this.richBoltNP);
                topologyBuilder.setBolt(this.boltsIdNP[1], this.richBoltNP);
                Assert.assertFalse("No exception was expected", this.expectedValueBoltsNP);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBoltsNP);
            }

        }

        @Test
        public void testSetBasicBoltNoParallelismHint() {
            try {
                topologyBuilder.setBolt(this.boltsIdNP[0], this.basicBoltNP);
                topologyBuilder.setBolt(this.boltsIdNP[1], this.basicBoltNP);
                Assert.assertFalse("No exception was expected", this.expectedValueBoltsNP);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBoltsNP);
            }

        }


        @Test
        public void testSetStatefulBoltNoParallelismHint() {
            try {
                topologyBuilder.setBolt(this.boltsIdNP[0], this.statefulBoltNP);
                topologyBuilder.setBolt(this.boltsIdNP[1], this.statefulBoltNP);
                Assert.assertFalse("No exception was expected", this.expectedValueBoltsNP);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBoltsNP);
            }

        }

        @Test
        public void testSetStatefulWindowedBoltNoParallelismHint() {
            try {
                topologyBuilder.setBolt(this.boltsIdNP[0], this.statefulWindowedBoltNP);
                topologyBuilder.setBolt(this.boltsIdNP[1], this.statefulWindowedBoltNP);
                Assert.assertFalse("No exception was expected", this.expectedValueBoltsNP);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBoltsNP);
            }

        }

        @Test
        public void testWindowedBoltNoParallelismHint() {
            try {
                topologyBuilder.setBolt(this.boltsIdNP[0], this.windowedBoltNP);
                topologyBuilder.setBolt(this.boltsIdNP[1], this.windowedBoltNP);
                Assert.assertFalse("No exception was expected", this.expectedValueBoltsNP);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBoltsNP);
            }
        }

        @Test
        public void testWindowedBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.windowedBolt, this.parallelismHintBolts);
                topologyBuilder.setBolt(this.boltsId[1], this.windowedBolt,this.parallelismHintBolts);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }



        @Test
        public void testSetBasicBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.basicBolt, this.parallelismHintBolts);
                topologyBuilder.setBolt(this.boltsId[1], this.basicBolt, this.parallelismHintBolts);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }

        }

        @Test
        public void testSetStatefulBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.statefulBolt, this.parallelismHintBolts);
                topologyBuilder.setBolt(this.boltsId[1], this.statefulBolt, this.parallelismHintBolts);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetStatefulWindowedBolt() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.statefulWindowedBolt, this.parallelismHintBolts);
                topologyBuilder.setBolt(this.boltsId[1], this.statefulWindowedBolt, this.parallelismHintBolts);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetStatefulWindowedBoltPersistent() {
            try {
                topologyBuilder.setBolt(this.boltsId[0], this.statefulWindowedBoltPersistent, this.parallelismHintBolts);
                topologyBuilder.setBolt(this.boltsId[1], this.statefulWindowedBoltPersistent, this.parallelismHintBolts);
                Assert.assertFalse("No exception was expected", this.expectedValueBolts);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueBolts);
            }
        }

        @Test
        public void testSetSpout() {
            try {
                topologyBuilder.setSpout(this.spoutsId[0], this.richSpout, this.parallelismHintSpout);
                topologyBuilder.setSpout(this.spoutsId[1], this.richSpout, this.parallelismHintSpout);
                Assert.assertFalse("No exception was expected", this.expectedValueSpout);

            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedValueSpout);
            }
        }

        @Test
        public void testSetSpoutSupplier() {
            try {
                topologyBuilder.setSpout(this.spoutsId[0], this.supplier, this.parallelismHintSpoutSupplier);
                topologyBuilder.setSpout(this.spoutsId[1], this.supplier, this.parallelismHintSpoutSupplier);
                Assert.assertFalse("No exception was expected", this.expectedSupplier);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("An exception is expected", this.expectedSupplier);
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

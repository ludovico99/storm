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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
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
        private List<ImmutableSet<GlobalStreamId>> expectedBoltsSet;
        private boolean exceptionInConfigPhase = false;

        private final String[] bolts = {"bolts1", "bolts2", "bolts3"};

        public TestTopologyCreation(ParamType spout, ParamType bolt, TopologyConfigEnum topologyConfig) {
            configure(spout, bolt, topologyConfig);
        }

        private void configure(ParamType spout, ParamType bolt, TopologyConfigEnum topologyConfig) {
            this.topologyBuilder = new TopologyBuilder();
            this.boltParamType = bolt;
            this.spoutParamType = spout;

            try {
                switch (spout) {
                    case VALID_INSTANCE:
                        break;
                    case INVALID_INSTANCE:
                        this.expectedException = true;
                        break;
                    case NULL_INSTANCE:
                        this.exceptionInConfigPhase = true;
                        break;
                }

                switch (bolt) {
                    case VALID_INSTANCE:
                        break;
                    case INVALID_INSTANCE:
                        this.expectedException = true;
                        break;
                    case NULL_INSTANCE:
                        this.exceptionInConfigPhase = true;
                        break;
                }

                String spouts = "spout1";
                switch (topologyConfig) {
                    case STATEFUL_BOLT:
                        this.topologyBuilder.setSpout(spouts, richSpout());

                        this.topologyBuilder.setBolt(bolts[0], richBolt(), 1)
                                .shuffleGrouping(spouts);
                        this.topologyBuilder.setBolt(bolts[1], statefulBolt(), 1)
                                .shuffleGrouping(bolts[0]);
                        this.topologyBuilder.setBolt(bolts[2], basicBolt(), 1)
                                .shuffleGrouping(bolts[1]).shuffleGrouping(spouts);

                        this.expectedSpoutsSet = ImmutableSet.of(spouts,"$checkpointspout");

                        this.expectedBoltsSet = new ArrayList<>();

                        this.expectedBoltsSet.add(ImmutableSet.of(
                                new GlobalStreamId(spouts, "default"),
                                new GlobalStreamId("$checkpointspout", "$checkpoint")));
                        this.expectedBoltsSet.add(ImmutableSet.of(
                                new GlobalStreamId(bolts[0], "default"),
                                new GlobalStreamId(bolts[0], "$checkpoint")));
                        this.expectedBoltsSet.add(ImmutableSet.of(
                                new GlobalStreamId(bolts[1], "default"),
                                new GlobalStreamId(bolts[1], "$checkpoint"),
                                new GlobalStreamId(spouts, "default"),
                                new GlobalStreamId("$checkpointspout", "$checkpoint")));

                        break;
                    case NO_STATEFUL_BOLT:
                        this.topologyBuilder.setSpout(spouts, richSpout());

                        this.topologyBuilder.setBolt(bolts[0], richBolt(), 1)
                                .shuffleGrouping(spouts);
                        this.topologyBuilder.setBolt(bolts[1], richBolt(), 1)
                                .shuffleGrouping(bolts[0]);
                        this.topologyBuilder.setBolt(bolts[2], basicBolt(), 1)
                                .shuffleGrouping(bolts[1]).shuffleGrouping(spouts);

                        this.expectedSpoutsSet = ImmutableSet.of(spouts);

                        this.expectedBoltsSet = new ArrayList<>();
                        this.expectedBoltsSet.add(ImmutableSet.of(new GlobalStreamId(spouts, "default")));
                        this.expectedBoltsSet.add(ImmutableSet.of(new GlobalStreamId(bolts[0], "default")));
                        this.expectedBoltsSet.add(ImmutableSet.of(
                                new GlobalStreamId(bolts[1], "default"),
                                new GlobalStreamId(spouts, "default")));

                        break;
                    case EMPTY_TOPOLOGY:
                        this.expectedSpoutsSet = emptySet();
                        this.expectedBoltsSet = emptyList();
                        this.expectedException = false;
                        break;

                }
            }catch (Exception e){
                e.printStackTrace();
                this.exceptionInConfigPhase = true;
            }

        }


        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    //SPOUT                      BOLT                        Topology con Stateful Bolts?
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE,   TopologyConfigEnum.STATEFUL_BOLT},
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE,   TopologyConfigEnum.EMPTY_TOPOLOGY},
                    {ParamType.VALID_INSTANCE,   ParamType.VALID_INSTANCE,   TopologyConfigEnum.NO_STATEFUL_BOLT},
                    {ParamType.VALID_INSTANCE,   ParamType.INVALID_INSTANCE, TopologyConfigEnum.STATEFUL_BOLT},
                    {ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE, TopologyConfigEnum.STATEFUL_BOLT},
                    {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE,   TopologyConfigEnum.STATEFUL_BOLT},
                    {ParamType.NULL_INSTANCE,    ParamType.VALID_INSTANCE,   TopologyConfigEnum.STATEFUL_BOLT},
                    {ParamType.VALID_INSTANCE,   ParamType.NULL_INSTANCE,    TopologyConfigEnum.STATEFUL_BOLT}
            });
        }

        private IRichBolt richBolt() {
            if (this.boltParamType == ParamType.INVALID_INSTANCE) return new BaseRichBolt() {
                @Override
                public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

                @Override
                public void execute(Tuple input) {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

            };
            else if (this.boltParamType == ParamType.VALID_INSTANCE)  return new BaseRichBolt() {
                    @Override
                    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

                    @Override
                    public void execute(Tuple input) {}

                    @Override
                    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                    private void writeObject(java.io.ObjectOutputStream stream) {}

                };
            else return null;

        }

        private IBasicBolt basicBolt() {
            if (this.boltParamType ==ParamType.INVALID_INSTANCE) return new BaseBasicBolt() {
                @Override
                public void execute(Tuple input, BasicOutputCollector collector) {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

            };
            else if (this.boltParamType == ParamType.VALID_INSTANCE)  {
                return new BaseBasicBolt() {
                    @Override
                    public void execute(Tuple input, BasicOutputCollector collector) {}

                    @Override
                    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                    private void writeObject(java.io.ObjectOutputStream stream) {}
                };
            }
            else return null;
        }

        private IStatefulBolt<?> statefulBolt() {
            if (this.boltParamType == ParamType.INVALID_INSTANCE) return new BaseStatefulBolt<State>() {
                @Override
                public void initState(State state) {}

                @Override
                public void execute(Tuple input) {}

            };
            else if (this.boltParamType == ParamType.VALID_INSTANCE)  {
                return new BaseStatefulBolt<State>() {
                    @Override
                    public void initState(State state) {}

                    @Override
                    public void execute(Tuple input) {}

                    private void writeObject(java.io.ObjectOutputStream stream) {}
                };
            }
            else return null;
        }

        private IRichSpout richSpout() {
            if (this.spoutParamType == ParamType.INVALID_INSTANCE) return new BaseRichSpout() {

                private String string;

                public void BaseRichSpout (){
                    this.string = "NOT_SERIALIZABLE";
                }
                @Override
                public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

                @Override
                public void nextTuple() {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {
                }
            };

            else if (this.spoutParamType == ParamType.VALID_INSTANCE)
                return new BaseRichSpout() {

                    @Override
                    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

                    @Override
                    public void nextTuple() {}

                    @Override
                    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                    private void writeObject(java.io.ObjectOutputStream stream) {}
                };

            else return null;
        }


        @Test
        public void test_createTopology() {
            try {
                if (exceptionInConfigPhase) {
                    Assert.assertTrue("An exception was raised by setBolt method", true);
                }
                else {
                    StormTopology topology = this.topologyBuilder.createTopology();

                    Set<String> spouts = topology.get_spouts().keySet();
                    Assert.assertEquals(this.expectedSpoutsSet, spouts);

                    if(this.expectedBoltsSet.isEmpty()){
                        Assert.assertEquals(0, topology.get_bolts().size());

                        Assert.assertEquals(0, topology.get_bolts().size());

                        Assert.assertEquals(0, topology.get_bolts().size());
                    }
                    else {
                        Assert.assertEquals(this.expectedBoltsSet.get(0), topology.get_bolts().get(this.bolts[0]).get_common().get_inputs().keySet());

                        Assert.assertEquals(this.expectedBoltsSet.get(1), topology.get_bolts().get(this.bolts[1]).get_common().get_inputs().keySet());

                        Assert.assertEquals(this.expectedBoltsSet.get(2), topology.get_bolts().get(this.bolts[2]).get_common().get_inputs().keySet());
                    }

                    Assert.assertFalse("No exception was expected", this.expectedException);
                }

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

            configureSetBolt((StringType) setBoltTest[0],(ParamType) setBoltTest[1], (ParamType) setBoltTest[2], (Boolean) setBoltTest[3]);

            configureSetSpout((StringType) setSpoutTest[0], (ParamType) setSpoutTest[1], (ParamType) setSpoutTest[2], (Boolean) setSpoutTest[3]);

            configureSetSpoutSupplier((StringType) setSpoutSupplier[0], (ParamType) setSpoutSupplier[1], (ParamType) setSpoutSupplier[2], (Boolean) setSpoutSupplier[3]);

            configureAddWorkerHook((ParamType) addWorkerHookTest[0], (Boolean) addWorkerHookTest[1]);

            configureNoParallelism((StringType) noParallelism[0], (ParamType) noParallelism[1], (Boolean) noParallelism[2]);

        }

        private void configureNoParallelism(StringType stringType, ParamType bolt, Boolean expectedValueBoltsNP) {
            this.expectedValueBoltsNP = expectedValueBoltsNP;

            switch (bolt) {
                case VALID_INSTANCE:
                    this.basicBoltNP = mock(IBasicBolt.class);
                    this.richBoltNP = mock(IRichBolt.class);
                    this.statefulBoltNP = mock(IStatefulBolt.class);
                    this.windowedBoltNP= mock(IWindowedBolt.class);
                    this.statefulWindowedBoltNP = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltNP.isPersistent()).thenReturn(false);
                    this.statefulWindowedBoltPersistentNP = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltPersistentNP.isPersistent()).thenReturn(true);
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsIdNP = new String[]{"bolt1", "bolt2"};
                            break;
                        case EMPTY_STRING:
                            this.boltsIdNP = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsIdNP = new String[]{null,null};
                            break;
                    }
                    break;
                case INVALID_INSTANCE:
                    this.basicBoltNP = mock(IBasicBolt.class);
                    this.richBoltNP = mock(IRichBolt.class);
                    this.statefulBoltNP = mock(IStatefulBolt.class);
                    this.windowedBoltNP=mock(IWindowedBolt.class);
                    this.statefulWindowedBoltNP = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltNP.isPersistent()).thenReturn(false);
                    this.statefulWindowedBoltPersistentNP = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltPersistentNP.isPersistent()).thenReturn(true);

                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsIdNP = new String[]{"bolt", "bolt"};
                            break;
                        case EMPTY_STRING:
                            this.boltsIdNP = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsIdNP = new String[]{null,null};
                            break;
                    }
                break;

                case NULL_INSTANCE:
                    this.basicBoltNP = null;
                    this.richBoltNP = null;
                    this.statefulBoltNP = null;
                    this.statefulWindowedBoltNP = null;
                    this.statefulWindowedBoltPersistentNP = null;
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsIdNP = new String[]{"bolt1", "bolt2"};
                            break;
                        case EMPTY_STRING:
                            this.boltsIdNP = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsIdNP = new String[]{null,null};
                            break;
                    }
                    break;
            }

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

        private void configureSetSpoutSupplier(StringType stringType, ParamType supplier, ParamType parallelismHint, Boolean expectedSupplier) {

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
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsIdNP = new String[]{"bolt1", "bolt2"};
                            break;
                        case EMPTY_STRING:
                            this.boltsIdNP = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsIdNP = new String[]{null,null};
                            break;
                    }
                    break;
                case INVALID_INSTANCE:
                    this.supplier = (SerializableSupplier<Object>) () -> null;
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsIdNP = new String[]{"bolt", "bolt"};
                            break;
                        case EMPTY_STRING:
                            this.boltsIdNP = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsIdNP = new String[]{null,null};
                            break;
                    }
                    break;
                case NULL_INSTANCE:
                    this.supplier = null;
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsIdNP = new String[]{"bolt1", "bolt2"};
                            break;
                        case EMPTY_STRING:
                            this.boltsIdNP = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsIdNP = new String[]{null,null};
                            break;
                    }
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

        private void configureSetSpout(StringType stringType, ParamType spout, ParamType parallelismHint, Boolean expectedValueSpout) {
            this.richSpout = mock(IRichSpout.class);
            this.expectedValueSpout = expectedValueSpout;

            switch (spout) {
                case VALID_INSTANCE:
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.spoutsId = new String[]{"spout1", "spout2"};
                            break;
                        case EMPTY_STRING:
                            this.spoutsId = new String[]{"", ""};
                            break;
                        case NULL:
                            this.spoutsId = new String[]{null,null};
                            break;
                    }
                    break;
                case INVALID_INSTANCE:
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.spoutsId = new String[]{"spout", "spout"};
                            break;
                        case EMPTY_STRING:
                            this.spoutsId = new String[]{"", ""};
                            break;
                        case NULL:
                            this.spoutsId = new String[]{null,null};
                            break;
                    }
                    break;
                case NULL_INSTANCE:
                    this.richSpout = null;
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.spoutsId = new String[]{"spout1", "spout2"};
                            break;
                        case EMPTY_STRING:
                            this.spoutsId = new String[]{"", ""};
                            break;
                        case NULL:
                            this.spoutsId = new String[]{null,null};
                            break;
                    }
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

        private void configureSetBolt(StringType stringType, ParamType bolt, ParamType parallelismHint, boolean expectedValueBolts) {

            this.expectedValueBolts = expectedValueBolts;

            switch (bolt) {
                case VALID_INSTANCE:
                    this.basicBolt = mock(IBasicBolt.class);
                    this.richBolt = mock(IRichBolt.class);
                    this.statefulBolt = mock(IStatefulBolt.class);
                    this.windowedBolt = mock(IWindowedBolt.class);
                    this.statefulWindowedBolt = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBolt.isPersistent()).thenReturn(false);

                    this.statefulWindowedBoltPersistent = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltPersistent.isPersistent()).thenReturn(true);
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsId = new String[]{"spout1", "spout2"};
                            break;
                        case EMPTY_STRING:
                            this.boltsId = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsId = new String[]{null,null};
                            break;
                    }
                    break;

                case INVALID_INSTANCE:
                    this.basicBolt = mock(IBasicBolt.class);
                    this.richBolt = mock(IRichBolt.class);
                    this.statefulBolt = mock(IStatefulBolt.class);
                    this.windowedBolt = mock(IWindowedBolt.class);
                    this.statefulWindowedBolt = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBolt.isPersistent()).thenReturn(false);

                    this.statefulWindowedBoltPersistent = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBoltPersistent.isPersistent()).thenReturn(true);
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsId = new String[]{"spout", "spout"};
                            break;
                        case EMPTY_STRING:
                            this.boltsId = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsId = new String[]{null,null};
                            break;
                    }
                    break;

                case NULL_INSTANCE:
                    this.basicBolt = null;
                    this.richBolt = null;
                    this.statefulBolt = null;
                    this.statefulWindowedBolt = null;
                    this.statefulWindowedBoltPersistent = null;
                    this.basicBolt = mock(IBasicBolt.class);
                    this.richBolt = mock(IRichBolt.class);
                    this.statefulBolt = mock(IStatefulBolt.class);
                    this.windowedBolt = mock(IWindowedBolt.class);
                    this.statefulWindowedBolt = mock(IStatefulWindowedBolt.class);
                    when(statefulWindowedBolt.isPersistent()).thenReturn(false);

                    this.statefulWindowedBoltPersistent = mock(IStatefulWindowedBolt.class);
                    switch (stringType){
                        case NO_EMPTY_STRING:
                            this.boltsId = new String[]{"spout", "spout"};
                            break;
                        case EMPTY_STRING:
                            this.boltsId = new String[]{"", ""};
                            break;
                        case NULL:
                            this.boltsId = new String[]{null,null};
                            break;
                    }
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

        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                                   //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] {StringType.NO_EMPTY_STRING, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, false},
                                         //BOLT,                    PaparallelismHint >0
                            new Object[] {StringType.NO_EMPTY_STRING,ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, false},
                            //IWorkerHook
                            new Object[] {ParamType.VALID_INSTANCE, false},
                                         //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {StringType.NO_EMPTY_STRING, ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, false},
                                        //BOLT
                            new Object[] {StringType.NO_EMPTY_STRING,ParamType.VALID_INSTANCE, false}
                    },

                                    //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] {StringType.EMPTY_STRING, ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},
                                            //BOLT,                    PaparallelismHint >0
                            new Object[] {StringType.EMPTY_STRING, ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, true},
                            //IWorkerHook
                            new Object[] {ParamType.INVALID_INSTANCE, true},
                                         //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {StringType.EMPTY_STRING, ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},
                                          //BOLT
                            new Object[] {StringType.EMPTY_STRING, ParamType.INVALID_INSTANCE, true}
                    },

                                    //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] { StringType.NULL,ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, true},
                                        //BOLT,                    PaparallelismHint >0
                            new Object[] {StringType.NULL, ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},
                            //IWorkerHook
                            new Object[] {ParamType.VALID_INSTANCE, false},
                            //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {StringType.NULL, ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, true},
                                         //BOLT
                            new Object[] {StringType.NULL, ParamType.INVALID_INSTANCE, true}
                    },

                                  //SPOUT,                   , PaparallelismHint >0,   Raise Exception?
                    {new Object[] {StringType.NO_EMPTY_STRING, ParamType.VALID_INSTANCE, ParamType.NULL_INSTANCE, false},
                            //BOLT,                    PaparallelismHint >0
                            new Object[] {StringType.NO_EMPTY_STRING,ParamType.NULL_INSTANCE, ParamType.VALID_INSTANCE, true},
                            //IWorkerHook
                            new Object[] {ParamType.NULL_INSTANCE, true},
                            //Serializable supplier,      , PaparallelismHint >0
                            new Object[] {StringType.NO_EMPTY_STRING,ParamType.VALID_INSTANCE, ParamType.NULL_INSTANCE, false},
                                        //BOLT
                            new Object[] {StringType.NO_EMPTY_STRING,ParamType.INVALID_INSTANCE, true}
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

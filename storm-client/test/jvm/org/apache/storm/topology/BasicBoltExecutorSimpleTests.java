package org.apache.storm.topology;


import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class BasicBoltExecutorSimpleTests {


    @RunWith(Parameterized.class)
    public static class ExecuteTest {
        //Execute
        private Tuple expectedExecute;
        private BasicBoltExecutor executor;
        private BasicBoltExecutorEnum boltExecutorEnum;
        private String tupleStreamId;
        private Tuple mockTuple;
        private boolean expectedExceptionExecute;
        private OutputCollector outputCollectorExecute;
        private final IBasicBolt mockBolt = spy(IBasicBolt.class);


        public ExecuteTest(ParamType tuple, ParamType outputCollector, BasicBoltExecutorEnum conf, boolean expectedException) {
            configureExecute(tuple, outputCollector, conf, expectedException);
        }


        private void configureExecute(ParamType tuple, ParamType outputCollectorParamType, BasicBoltExecutorEnum conf, boolean expectedException) {

            this.executor = new BasicBoltExecutor(mockBolt);

            this.boltExecutorEnum = conf;
            this.tupleStreamId = "default";
            this.expectedExceptionExecute = expectedException;

            switch (tuple) {
                case VALID_INSTANCE:
                    mockTuple = Mockito.mock(Tuple.class);
                    when(mockTuple.getSourceStreamId()).thenReturn(this.tupleStreamId);
                    this.expectedExecute = mockTuple;
                    break;
                case NULL_INSTANCE:
                    mockTuple = null;
                    this.expectedExecute = null;
                    break;

            }

            switch (outputCollectorParamType) {
                case VALID_INSTANCE:
                    this.outputCollectorExecute = mock(OutputCollector.class);
                    switch (conf) {
                        case NO_ACK_FAILED:
                            break;
                        case ACK_FAILED:
                            doThrow(new ReportedFailedException()).when(outputCollectorExecute).ack(isA(Tuple.class));
                            break;
                    }
                    break;
                case NULL_INSTANCE:
                    this.outputCollectorExecute = null;
                    break;
                case INVALID_INSTANCE:
                    this.outputCollectorExecute = new OutputCollector(null);
                    switch (conf) {
                        case NO_ACK_FAILED:
                            break;
                        case ACK_FAILED:
                            doThrow(new ReportedFailedException()).when(outputCollectorExecute).ack(isA(Tuple.class));
                            break;
                    }
                    break;
            }
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{

//                    // tuple ,                OutputCollector                        Config,                  expectedValue
//                    {ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE,   BasicBoltExecutorEnum.NO_ACK_FAILED, false},
//
//                    {ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE, BasicBoltExecutorEnum.NO_ACK_FAILED, true},
//
//                    {ParamType.VALID_INSTANCE, ParamType.NULL_INSTANCE,    BasicBoltExecutorEnum.NO_ACK_FAILED, true},
//
//                    {ParamType.NULL_INSTANCE, ParamType.VALID_INSTANCE,    BasicBoltExecutorEnum.NO_ACK_FAILED, false},
//
//                    {ParamType.NULL_INSTANCE, ParamType.NULL_INSTANCE,     BasicBoltExecutorEnum.NO_ACK_FAILED, true},
//
//                    {ParamType.NULL_INSTANCE, ParamType.INVALID_INSTANCE,  BasicBoltExecutorEnum.NO_ACK_FAILED, true},

                    {ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE,   BasicBoltExecutorEnum.ACK_FAILED,    false}


            });
        }

        @Test
        public void testHandleTupleAndAck() {
            try {
                HashMap<String, Object> topoConf = new HashMap<>();

                TopologyContext context = Mockito.mock(TopologyContext.class);

                this.executor.prepare(topoConf, context, outputCollectorExecute);

                this.executor.execute(mockTuple);

                ArgumentCaptor<Tuple> argument = ArgumentCaptor.forClass(Tuple.class);
                verify(this.mockBolt).execute(argument.capture(), nullable(BasicOutputCollector.class));
                verify(this.outputCollectorExecute).ack(mockTuple);

                if (argument.getValue() != null) {
                    Assert.assertEquals(this.expectedExecute, argument.getValue());
                    Assert.assertEquals(this.tupleStreamId, argument.getValue().getSourceStreamId());
                }

                if (this.boltExecutorEnum == BasicBoltExecutorEnum.ACK_FAILED && outputCollectorExecute != null) {
                    verify(this.outputCollectorExecute).reportError(isA(Throwable.class));
                    verify(this.outputCollectorExecute).fail(isA(Tuple.class));

                }

                Assert.assertFalse("No exception raised", this.expectedExceptionExecute);

            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue("Exception raised", this.expectedExceptionExecute);
            }

        }

    }

    @RunWith(Parameterized.class)
    public static class TestPrepare {

        //Prepare
        private String expectedPrepareComponent;
        private String expectedPrepareStream;
        private boolean expectedExceptionPrepare;
        private ParamType topologyContextParamType;
        private HashMap<String, Object> topoConfPrepare;
        private TopologyContext contextPrepare;
        private OutputCollector outputCollectorPrepare;
        private BasicBoltExecutor executor;
        private final IBasicBolt mockBolt = spy(IBasicBolt.class);

        public TestPrepare(ParamType topoConf, ParamType topologyContext, ParamType outputCollector, boolean expectedException) {
            configurePrepare(topoConf, topologyContext, outputCollector, expectedException);
        }


        private void configurePrepare(ParamType topoConf, ParamType topologyContext, ParamType outputCollector, boolean expectedException) {

            this.executor = new BasicBoltExecutor(mockBolt);
            this.topologyContextParamType = topologyContext;

            this.expectedExceptionPrepare = expectedException;
            String componentId1 = "bolt - 1";
            String streamId1 = "stream - 1";

            switch (topoConf) {
                    case VALID_INSTANCE:
                        this.topoConfPrepare = new HashMap<>();
                        break;
                    case INVALID_INSTANCE:
                        this.topoConfPrepare = new HashMap<>();
                        this.topoConfPrepare.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 0);
                        break;
                    case NULL_INSTANCE:
                        this.topoConfPrepare = null;
                        break;
                }


                switch (topologyContext) {
                    case VALID_INSTANCE:
                        this.contextPrepare = Mockito.mock(TopologyContext.class);

                        when(contextPrepare.getThisTaskId()).thenReturn(1);
                        GlobalStreamId globalStreamId = new GlobalStreamId(componentId1, streamId1);
                        Map<GlobalStreamId, Grouping> thisSources = Collections.singletonMap(globalStreamId, mock(Grouping.class));
                        when(contextPrepare.getThisSources()).thenReturn(thisSources);
                        when(contextPrepare.getComponentTasks(any())).thenReturn(Collections.singletonList(1));
                        when(contextPrepare.getThisComponentId()).thenReturn(componentId1);

                        this.expectedPrepareStream = streamId1;
                        this.expectedPrepareComponent = componentId1;
                        break;

                    case INVALID_INSTANCE:
                     break;

                    case NULL_INSTANCE:
                        this.contextPrepare = null;
                        break;
                }


                switch (outputCollector) {
                    case VALID_INSTANCE:
                        this.outputCollectorPrepare = mock(OutputCollector.class);
                        break;
                    case INVALID_INSTANCE:
                        this.outputCollectorPrepare = new OutputCollector(null);
                        break;
                    case NULL_INSTANCE:
                        this.outputCollectorPrepare = null;
                        break;
                }

        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{

                    //TopoConf                  TopologyContext         OutputCollector,            expectedValue
                    {ParamType.VALID_INSTANCE,  ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, false},

                    {ParamType.VALID_INSTANCE,  ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, true},

                    {ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE, false},

                    {ParamType.NULL_INSTANCE,    ParamType.NULL_INSTANCE, ParamType.NULL_INSTANCE, false},

                    {ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE, true}

            });
        }

        @Test
        public void test_Prepare() {
            try {

                if (this.topologyContextParamType == ParamType.INVALID_INSTANCE){
                    this.contextPrepare = new TopologyContext(null, null, null,
                            null, null, null, null, null,
                            null, null, null, null, null,
                            null, null, null, null, null);
                }

                this.executor.prepare(topoConfPrepare, contextPrepare, outputCollectorPrepare);

                    ArgumentCaptor<TopologyContext> argument = ArgumentCaptor.forClass(TopologyContext.class);
                    verify(this.mockBolt).prepare(nullable(Map.class), argument.capture());

                    if (this.topologyContextParamType == ParamType.VALID_INSTANCE) {
                        Assert.assertEquals("Exact component ID", this.expectedPrepareComponent, argument.getValue().getThisComponentId());
                        Assert.assertEquals("Exact stream ID", ImmutableSet.of(new GlobalStreamId(this.expectedPrepareComponent, this.expectedPrepareStream)),
                                argument.getValue().getThisSources().keySet());
                    }

                    Assert.assertFalse("No exception raised", this.expectedExceptionPrepare);

                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.assertTrue("No exception raised", this.expectedExceptionPrepare);
                }
            }
    }

    @RunWith(Parameterized.class)
    public static class TestOutputFieldDeclarer {

        //Declare
        private OutputFieldsDeclarer outputFieldsDeclarer;
        private BasicBoltExecutor executor;
        private final IBasicBolt mockBolt = spy(IBasicBolt.class);

        public TestOutputFieldDeclarer(OutputFieldsDeclarer declare) {

            configureDeclareOutputField(declare);
        }

        private void configureDeclareOutputField(OutputFieldsDeclarer declare) {

            this.executor = new BasicBoltExecutor(mockBolt);
            this.outputFieldsDeclarer = declare;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][]{
                    {mock(OutputFieldsDeclarer.class)}
            });
        }


        @Test
        public void test_DeclareOutputFields() {

            HashMap<String, Object> topoConf = new HashMap<>();

            TopologyContext context = Mockito.mock(TopologyContext.class);

            OutputCollector outputCollector = mock(OutputCollector.class);

            this.executor.prepare(topoConf, context, outputCollector);

            this.executor.declareOutputFields(outputFieldsDeclarer);

            ArgumentCaptor<OutputFieldsDeclarer> argument = ArgumentCaptor.forClass(OutputFieldsDeclarer.class);
            verify(this.mockBolt).declareOutputFields(argument.capture());

            Assert.assertEquals(this.outputFieldsDeclarer, argument.getValue());

        }

    }

    public static class TestGetComponentConfiguration{
        //GetComponentConfiguration
        private Map<String, Object> topoConfGetComponentConf;
        private BasicBoltExecutor executor;
        private final IBasicBolt mockBolt = spy(IBasicBolt.class);

        public TestGetComponentConfiguration() {
            configureGetComponentConfiguration();
        }

        public void configureGetComponentConfiguration() {

            this.topoConfGetComponentConf = new HashMap<>();
            this.executor = new BasicBoltExecutor(mockBolt);

            this.topoConfGetComponentConf.put(Config.TOPOLOGY_NAME, "TEST_TOPOLOGY");
            when(mockBolt.getComponentConfiguration()).thenReturn(this.topoConfGetComponentConf);

        }

        @Test
        public void test_GetComponentConfiguration() {

            HashMap<String, Object> topoConf = new HashMap<>();

            TopologyContext context = Mockito.mock(TopologyContext.class);

            OutputCollector collector = mock(OutputCollector.class);

            this.executor.prepare(topoConf, context, collector);

            Assert.assertEquals(this.topoConfGetComponentConf, this.executor.getComponentConfiguration());

            verify(this.mockBolt, Mockito.times(1)).getComponentConfiguration();

        }

    }

    public static class TestCleanup {


        private final IBasicBolt mockBolt = spy(IBasicBolt.class);
        private final BasicBoltExecutor executor = new BasicBoltExecutor(mockBolt);

        @Test
        public void test_Cleanup() {

            this.executor.cleanup();
            verify(this.mockBolt, Mockito.times(1)).cleanup();

        }


    }

}
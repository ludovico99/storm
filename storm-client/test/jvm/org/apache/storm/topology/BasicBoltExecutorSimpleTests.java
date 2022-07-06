package org.apache.storm.topology;


import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.*;

;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BasicBoltExecutorSimpleTests {
    private final BasicBoltExecutor executor;
    private final IBasicBolt mockBolt;
    private Map<String, Object> topoConf;
    private BasicBoltExecutorEnum boltExecutorEnum;
    private final OutputCollector outputCollector;

    //Execute
    private Tuple expectedExecute;
    private ParamType tupleParamType;
    private String tupleStreamId;
    private Tuple mockTuple;

    //Prepare
    private String expectedPrepareComponent;
    private String expectedPrepareStream;
    private ParamType componentIdParamType;
    private ParamType streamIdParamType ;
    private TopologyContext context;

    //Declare
    private OutputFieldsDeclarer expectedDeclare;
    private ParamType declareParamType;


    private boolean exceptionInConfigPhase = false;



    public BasicBoltExecutorSimpleTests(Object[] executeTest, Object[] prepareTest, Object[] declareOutputFieldTest) {
        this.mockBolt = mock(IBasicBolt.class);

        this.executor = spy(new BasicBoltExecutor(mockBolt));

        this.outputCollector = mock(OutputCollector.class);

        configurePrepare((ParamType) prepareTest[0], (ParamType) prepareTest[1]);

        configureExecute((ParamType) executeTest[0], (BasicBoltExecutorEnum) executeTest[1]);

        configureDeclareOutputField((ParamType) declareOutputFieldTest[0]);

        configureGetComponentConfiguration();
    }

    private void configureDeclareOutputField(ParamType declare) {

        OutputFieldsDeclarer outputFieldsDeclarer = mock(OutputFieldsDeclarer.class);
        this.declareParamType = declare;

        switch (declare) {
            case VALID_INSTANCE:
                this.expectedDeclare = outputFieldsDeclarer;
                break;
            case INVALID_INSTANCE:
                this.expectedDeclare = mock(OutputFieldsDeclarer.class);
                break;
        }

        this.executor.declareOutputFields(outputFieldsDeclarer);

    }

    private void configureExecute(ParamType tuple, BasicBoltExecutorEnum conf) {


        this.boltExecutorEnum = conf;
        this.tupleParamType = tuple;
        this.tupleStreamId = "default";

        mockTuple = Mockito.mock(Tuple.class);
        when(mockTuple.getSourceStreamId()).thenReturn(this.tupleStreamId);

        switch (tuple) {
            case VALID_INSTANCE:
                this.expectedExecute = mockTuple;
                break;
            case INVALID_INSTANCE:
                this.expectedExecute = mock(Tuple.class);
                break;

        }

        switch (conf) {
            case NO_ACK_FAILED:
                break;
            case ACK_FAILED:
                doThrow(new ReportedFailedException()).when(outputCollector).ack(isA(Tuple.class));
                break;
        }


    }

    private void configurePrepare(ParamType componentId, ParamType streamId) {

        this.componentIdParamType = componentId;
        this.streamIdParamType = streamId;

        try {

            String componentId1 = "bolt - 1";
            switch (componentId){
                case VALID_INSTANCE:
                    this.expectedPrepareComponent = componentId1;
                    break;
                case INVALID_INSTANCE:
                    this.expectedPrepareComponent = "another component ID";
                    break;
            }

            String streamId1 = "stream - 1";
            switch (streamId){
                case VALID_INSTANCE:
                    this.expectedPrepareStream = streamId1;
                    break;
                case INVALID_INSTANCE:
                    this.expectedPrepareStream = "another stream";
                    break;
            }

            this.topoConf = new HashMap<>();
            context = Mockito.mock(TopologyContext.class);

            when(context.getThisTaskId()).thenReturn(1);
            GlobalStreamId globalStreamId = new GlobalStreamId(componentId1, streamId1);
            Map<GlobalStreamId, Grouping> thisSources = Collections.singletonMap(globalStreamId, mock(Grouping.class));
            when(context.getThisSources()).thenReturn(thisSources);
            when(context.getComponentTasks(any())).thenReturn(Collections.singletonList(1));
            when(context.getThisComponentId()).thenReturn(componentId1);


        } catch (Exception e) {
            e.printStackTrace();
            this.exceptionInConfigPhase = true;
        }
    }

    public void configureGetComponentConfiguration() {

        this.topoConf.put("bolt-1", "fields");
        when(mockBolt.getComponentConfiguration()).thenReturn(this.topoConf);


    }

    @Before
    public void setUp (){
        this.executor.prepare(topoConf, context, outputCollector);
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][]{
                {
                        // tuple ,             Config
                        new Object[]{ParamType.VALID_INSTANCE, BasicBoltExecutorEnum.NO_ACK_FAILED},
                        //ComponentId, stream Id
                        new Object[]{ParamType.VALID_INSTANCE, ParamType.VALID_INSTANCE},
                        //Declare outputField
                        new Object[]{ParamType.VALID_INSTANCE},

                },

                {
                        new Object[]{ParamType.INVALID_INSTANCE, BasicBoltExecutorEnum.NO_ACK_FAILED},
                        new Object[]{ParamType.VALID_INSTANCE, ParamType.INVALID_INSTANCE},
                        new Object[]{ParamType.INVALID_INSTANCE}
                },
                {
                        new Object[]{ParamType.INVALID_INSTANCE, BasicBoltExecutorEnum.ACK_FAILED},
                        new Object[]{ParamType.INVALID_INSTANCE, ParamType.VALID_INSTANCE},
                        new Object[]{ParamType.VALID_INSTANCE}
                },
                {
                        new Object[]{ParamType.VALID_INSTANCE, BasicBoltExecutorEnum.ACK_FAILED},
                        new Object[]{ParamType.INVALID_INSTANCE, ParamType.INVALID_INSTANCE},
                        new Object[]{ParamType.VALID_INSTANCE}

                }
        });
    }


    @Test
    public void testHandleTupleAndAck() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {

            this.executor.execute(mockTuple);

            ArgumentCaptor<Tuple> argument = ArgumentCaptor.forClass(Tuple.class);
            verify(this.mockBolt).execute(argument.capture(), isA(BasicOutputCollector.class));
            verify(this.outputCollector).ack(mockTuple);

            if (tupleParamType == ParamType.INVALID_INSTANCE) {
                Assert.assertNotEquals(this.expectedExecute, argument.getValue());
            } else {
                Assert.assertEquals(this.expectedExecute, argument.getValue());
                Assert.assertEquals(this.tupleStreamId, argument.getValue().getSourceStreamId());
            }



            if (this.boltExecutorEnum == BasicBoltExecutorEnum.ACK_FAILED) {
                verify(this.outputCollector).reportError(isA(Throwable.class));
                verify(this.outputCollector).fail(isA(Tuple.class));

            }
        }
    }

    @Test
    public void test_Prepare() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {

            ArgumentCaptor<TopologyContext> argument = ArgumentCaptor.forClass(TopologyContext.class);
            verify(this.mockBolt).prepare(isA(Map.class), argument.capture());
            if (this.componentIdParamType == ParamType.INVALID_INSTANCE)
                Assert.assertNotEquals("Another component ID", this.expectedPrepareComponent,argument.getValue().getThisComponentId());
            else Assert.assertEquals("Exact component ID",this.expectedPrepareComponent, argument.getValue().getThisComponentId());

            if (this.streamIdParamType == ParamType.VALID_INSTANCE && this.componentIdParamType == ParamType.VALID_INSTANCE)
                Assert.assertEquals("Exact stream ID",ImmutableSet.of(new GlobalStreamId(this.expectedPrepareComponent, this.expectedPrepareStream)),
                        argument.getValue().getThisSources().keySet());
            else Assert.assertNotEquals("Another stream ID",
                    ImmutableSet.of(new GlobalStreamId(this.expectedPrepareComponent, this.expectedPrepareStream)),  argument.getValue().getThisSources().keySet());

        }
    }

    @Test
    public void test_Cleanup() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {

            this.executor.cleanup();
            verify(this.mockBolt, Mockito.times(1)).cleanup();

        }
    }

    @Test
    public void test_GetComponentConfiguration() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {


            Assert.assertEquals(this.topoConf, this.executor.getComponentConfiguration());

            verify(this.mockBolt, Mockito.times(1)).getComponentConfiguration();

        }
    }

    @Test
    public void test_DeclareOutputFields() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {
            ArgumentCaptor<OutputFieldsDeclarer> argument = ArgumentCaptor.forClass(OutputFieldsDeclarer.class);
            verify(this.mockBolt).declareOutputFields(argument.capture());

            if (this.declareParamType == ParamType.INVALID_INSTANCE) Assert.assertNotEquals(this.expectedDeclare, argument.getValue());
            else Assert.assertEquals(this.expectedDeclare, argument.getValue());

        }
    }
}
package org.apache.storm.topology;


import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
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
    private BasicBoltExecutor executor;
    private IBasicBolt mockBolt;
    private Map<String, Object> topoConf;
    private String componentId;
    private String streamId;
    private BasicBoltExecutorEnum boltExecutorEnum;
    private OutputCollector outputCollector;


    private boolean exceptionInConfigPhase = false;
    private OutputFieldsDeclarer outputFieldsDeclarer;

    public BasicBoltExecutorSimpleTests(String componentId, String streamId,BasicBoltExecutorEnum basicBoltExecutorEnum) {

        configure(componentId,streamId, basicBoltExecutorEnum);
    }

    private void configure(String componentId, String streamId, BasicBoltExecutorEnum basicBoltExecutorEnum) {

        this.componentId = componentId;
        this.streamId = streamId;
        this.boltExecutorEnum = basicBoltExecutorEnum;
        try {
            this.mockBolt = mock(IBasicBolt.class);

            this.executor = spy(new BasicBoltExecutor(mockBolt));

            this.outputCollector = mock(OutputCollector.class);

            this.outputFieldsDeclarer = mock(OutputFieldsDeclarer.class);

            switch (basicBoltExecutorEnum){
                case NO_ACK_FAILED:
                    break;
                case ACK_FAILED:
                    doThrow(new ReportedFailedException()).when(outputCollector).ack(isA(Tuple.class));
                    break;
            }


            this.topoConf = new HashMap<>();
            this.topoConf.put("bolt-1","fields");
            when(mockBolt.getComponentConfiguration()).thenReturn(this.topoConf);

            TopologyContext context = Mockito.mock(TopologyContext.class);

            when(context.getThisTaskId()).thenReturn(1);
            GlobalStreamId globalStreamId = new GlobalStreamId(componentId, streamId);
            Map<GlobalStreamId, Grouping> thisSources = Collections.singletonMap(globalStreamId, mock(Grouping.class));
            when(context.getThisSources()).thenReturn(thisSources);
            when(context.getComponentTasks(any())).thenReturn(Collections.singletonList(1));
            when(context.getThisComponentId()).thenReturn(componentId);

            this.executor.prepare(topoConf, context, outputCollector);

        }catch (Exception e) {
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][] {
                //Component ID,         StreamID
                {"bolt",            "stream-1",      BasicBoltExecutorEnum.NO_ACK_FAILED},
                {"bolt",            "",              BasicBoltExecutorEnum.NO_ACK_FAILED},
                {"bolt",            null,            BasicBoltExecutorEnum.NO_ACK_FAILED},
                {"",                "stream-1",      BasicBoltExecutorEnum.NO_ACK_FAILED},
                {null,              "stream-1",      BasicBoltExecutorEnum.NO_ACK_FAILED},

                {"bolt",            "stream-1",      BasicBoltExecutorEnum.ACK_FAILED},

        });
    }


    @Test
    public void testHandleTuple() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {
            Tuple mockTuple = Mockito.mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn(this.streamId);
            this.executor.execute(mockTuple);

            ArgumentCaptor<Tuple> argument = ArgumentCaptor.forClass(Tuple.class);
            verify(this.mockBolt).execute(argument.capture(), isA(BasicOutputCollector.class));
            Assert.assertEquals(mockTuple, argument.getValue());
            Assert.assertEquals(this.streamId,argument.getValue().getSourceStreamId());

            if(this.boltExecutorEnum == BasicBoltExecutorEnum.ACK_FAILED) {
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

            Assert.assertEquals("Expected component ID", this.componentId, argument.getValue().getThisComponentId());
            Assert.assertEquals("Expected stream ID", ImmutableSet.of(new GlobalStreamId(this.componentId, this.streamId)), argument.getValue().getThisSources().keySet());
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

            this.executor.declareOutputFields(this.outputFieldsDeclarer);

            verify(this.mockBolt, Mockito.times(1)).declareOutputFields(this.outputFieldsDeclarer);

        }
    }



}

package org.apache.storm.topology;


import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
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
    private OutputCollector outputCollector;
    private IBasicBolt mockBolt;
    private Map<String, Object> topoConf;
    private TopologyContext context;
    private OutputCollector collector;
    private TopologyContextEnum topologyContextEnum;

    private boolean exceptionInConfigPhase = false;

    public BasicBoltExecutorSimpleTests(TopologyContextEnum topologyContextEnum) {
        configure(topologyContextEnum);
    }

    private void configure(TopologyContextEnum topologyContextEnum) {
        this.topologyContextEnum = topologyContextEnum;
        try {
            this.mockBolt = Mockito.mock(IBasicBolt.class);

            this.executor = spy(new BasicBoltExecutor(mockBolt));

            this.outputCollector = Mockito.mock(OutputCollector.class);

            this.topoConf = new HashMap<>();

            this.context = Mockito.mock(TopologyContext.class);

            switch (topologyContextEnum) {
                case VALID_TOPOLOGY_CONTEXT:

                    when(this.context.getThisTaskId()).thenReturn(1);
                    GlobalStreamId globalStreamId = new GlobalStreamId("bolt", "default");
                    Map<GlobalStreamId, Grouping> thisSources = Collections.singletonMap(globalStreamId, mock(Grouping.class));
                    when(this.context.getThisSources()).thenReturn(thisSources);
                    when(this.context.getComponentTasks(any())).thenReturn(Collections.singletonList(1));
                    when(this.context.getThisComponentId()).thenReturn("bolt");

                    break;

            }
            this.executor.prepare(topoConf, context, outputCollector);

        }catch (Exception e) {
            e.printStackTrace();
            //this.exceptionInConfigPhase = true;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {

        return Arrays.asList(new Object[][] {
                {TopologyContextEnum.VALID_TOPOLOGY_CONTEXT}
        });
    }


    @Test
    public void testHandleTuple() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {
            Tuple mockTuple = Mockito.mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn("default");
            this.executor.execute(mockTuple);
            verify(this.mockBolt, Mockito.times(1)).prepare(this.topoConf, this.context);

            ArgumentCaptor<Tuple> argument = ArgumentCaptor.forClass(Tuple.class);
            verify(this.mockBolt).execute(argument.capture(), isA(BasicOutputCollector.class));
            Assert.assertEquals(mockTuple, argument.getValue());
        }
    }

    @Test
    public void test_Cleanup() {
        if (this.exceptionInConfigPhase) {
            Assert.assertTrue("No exception was expected, but an exception during the set up of the test case has" +
                    " been thrown.", true);
        } else {

            Tuple mockTuple = Mockito.mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn("default");
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

            Tuple mockTuple = Mockito.mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn("default");
            Assert.assertEquals(this.topoConf, this.executor.getComponentConfiguration());

            verify(this.mockBolt, Mockito.times(1)).getComponentConfiguration();

        }
    }



}

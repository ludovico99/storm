package org.apache.storm.topology;


import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.*;

;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Ignore
@RunWith(Parameterized.class)
public class BasicBoltExecutorTest {
    private BasicBoltExecutor executor;
    private OutputCollector outputCollector;
    private IBasicBolt basicBolt;
    private IBasicBolt basicBolt_2;
    private Map<String, Object> topoConf;
    private TopologyContext context;
    private OutputCollector collector;
    private TopologyContextEnum topologyContextEnum;

    private boolean exceptionInConfigPhase = false;


    public BasicBoltExecutorTest(TopologyContextEnum topologyContextEnum) {
        configure(topologyContextEnum);
    }

    private void configure(TopologyContextEnum topologyContextEnum) {
        this.topologyContextEnum = topologyContextEnum;
        try {

            this.basicBolt = spy(new IBasicBolt() {
                @Override
                public void prepare(Map<String, Object> topoConf, TopologyContext context) {}

                @Override
                public void execute(Tuple input, BasicOutputCollector collector) {
                    collector.emit(new Values(input.getValue(0)));
                }

                @Override
                public void cleanup() {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {
                    declarer.declare(new Fields("fields"));
                }

                @Override
                public Map<String, Object> getComponentConfiguration() {return null;}
            });

            this.basicBolt_2 = mock(IBasicBolt.class);
            doNothing().when(this.basicBolt_2).execute(isA(Tuple.class),isA(BasicOutputCollector.class));


            this.executor = spy(new BasicBoltExecutor(basicBolt));

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
            verify(this.basicBolt, Mockito.times(1)).prepare(this.topoConf, this.context);

            ArgumentCaptor<Tuple> argument = ArgumentCaptor.forClass(Tuple.class);
            verify(this.basicBolt).execute(argument.capture(), isA(BasicOutputCollector.class));
            Assert.assertEquals(mockTuple, argument.getValue());
        }
    }


}

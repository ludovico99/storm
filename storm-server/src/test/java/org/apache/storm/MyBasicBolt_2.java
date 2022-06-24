package org.apache.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MyBasicBolt_2 implements IBasicBolt {
    public static int received;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {}

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        received = (int) input.getValue(0);
        collector.emit(new Values(input.getValue(0)));
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fields"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

package org.apache.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MyBasicBolt implements IBasicBolt {

    public static int added;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {}

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        added = 50;
        int toSend = (int) input.getValue(0) + added;
        collector.emit(new Values(toSend));
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

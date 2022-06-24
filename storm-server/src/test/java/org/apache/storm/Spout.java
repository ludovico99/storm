package org.apache.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.CompletableSpout;;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Spout extends BaseRichSpout implements CompletableSpout {

    SpoutOutputCollector collector;

    @Override
    public boolean isExhausted() {
        return false;
    }

    @Override
    public void clean() {
        CompletableSpout.super.clean();
    }

    @Override
    public void startup() {
        CompletableSpout.super.startup();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(100));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

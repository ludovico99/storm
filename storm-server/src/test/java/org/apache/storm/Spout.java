package org.apache.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.CompletableSpout;;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout implements CompletableSpout {

    SpoutOutputCollector collector;
    public static int toSend;

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
        toSend = 50;
        this.collector.emit(new Values(toSend));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fields"));
    }
}

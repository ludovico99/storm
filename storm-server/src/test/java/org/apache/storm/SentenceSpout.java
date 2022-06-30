package org.apache.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.CompletableSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;


public class SentenceSpout extends BaseRichSpout implements CompletableSpout {
    SpoutOutputCollector _collector;

    int i;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        _collector = collector;
        i = 0;
    }


    @Override
    public void nextTuple() {

        Utils.sleep(100);

        String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };

        String sentence = sentences[i%5];
        i++;
        _collector.emit(new Values(sentence));
    }


    @Override
    public void ack(Object id) {}

    @Override
    public void fail(Object id) {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

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
}

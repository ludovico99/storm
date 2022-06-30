package org.apache.storm;

import java.util.HashMap;
import java.util.Map;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


// For logging
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class WordCount extends BaseBasicBolt {
    private static final Logger logger = LogManager.getLogger(WordCount.class);
    static Map<String, Integer> counts = new HashMap<String, Integer>();

    private Integer emitFrequency;

    public WordCount() {
        emitFrequency=5;
    }

    public WordCount(Integer frequency) {
        emitFrequency=frequency;
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            for(String word : counts.keySet()) {
                Integer count = counts.get(word);
                collector.emit(new Values(word, count));
                logger.info("Emitting a count of " + count + " for word " + word);
            }
        } else {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }



}
package org.apache.storm.topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public class Utils {

    public Utils() {}

    public IRichBolt richBolt(ParamType boltParamType) {
        if (boltParamType == ParamType.INVALID_INSTANCE) return new BaseRichBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

            @Override
            public void execute(Tuple input) {}

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {}

        };
        else if (boltParamType == ParamType.VALID_INSTANCE)  return new BaseRichBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

            @Override
            public void execute(Tuple input) {}

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {}

            private void writeObject(java.io.ObjectOutputStream stream) {}

        };
        else return null;

    }

    public IBasicBolt basicBolt(ParamType boltParamType) {
        if (boltParamType ==ParamType.INVALID_INSTANCE) return new BaseBasicBolt() {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector) {}

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {}

        };
        else if (boltParamType == ParamType.VALID_INSTANCE)  {
            return new BaseBasicBolt() {
                @Override
                public void execute(Tuple input, BasicOutputCollector collector) {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
        }
        else return null;
    }

    public IStatefulBolt<?> statefulBolt(ParamType boltParamType) {
        if (boltParamType == ParamType.INVALID_INSTANCE) return new BaseStatefulBolt<State>() {
            @Override
            public void initState(State state) {}

            @Override
            public void execute(Tuple input) {}

        };
        else if (boltParamType == ParamType.VALID_INSTANCE)  {
            return new BaseStatefulBolt<State>() {
                @Override
                public void initState(State state) {}

                @Override
                public void execute(Tuple input) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
        }
        else return null;
    }

    public IRichSpout richSpout(ParamType spoutParamType) {
        if (spoutParamType == ParamType.INVALID_INSTANCE) return new BaseRichSpout() {

            private String string;

            public void BaseRichSpout (){
                this.string = "NOT_SERIALIZABLE";
            }
            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

            @Override
            public void nextTuple() {}

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        };

        else if (spoutParamType == ParamType.VALID_INSTANCE)
            return new BaseRichSpout() {
                @Override
                public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

                @Override
                public void nextTuple() {}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                private void writeObject(java.io.ObjectOutputStream stream) {}
            };

        else return null;
    }

    public IWindowedBolt windowedBolt(ParamType boltParamType){
        if (boltParamType == ParamType.INVALID_INSTANCE) return new IWindowedBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

            @Override
            public void execute(TupleWindow inputWindow) {}

            @Override
            public void cleanup() {}

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;}

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {}

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

        };
        else if (boltParamType == ParamType.VALID_INSTANCE)  {
            return new IWindowedBolt() {
                @Override
                public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

                @Override
                public void execute(TupleWindow inputWindow) {}

                @Override
                public void cleanup() {}

                @Override
                public TimestampExtractor getTimestampExtractor() {
                    return null;}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                @Override
                public Map<String, Object> getComponentConfiguration() {
                    return null;}


                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
        }
        else return null;
    }

    public  IStatefulWindowedBolt statefulWindowedBolt(ParamType boltParamType){
        if (boltParamType == ParamType.INVALID_INSTANCE) return new IStatefulWindowedBolt() {
            @Override
            public boolean isPersistent() {
                return IStatefulWindowedBolt.super.isPersistent();}

            @Override
            public long maxEventsInMemory() {
                return IStatefulWindowedBolt.super.maxEventsInMemory();}

            @Override
            public void initState(State state) {}

            @Override
            public void preCommit(long txid) {}

            @Override
            public void prePrepare(long txid) {}

            @Override
            public void preRollback() {}

            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

            @Override
            public void execute(TupleWindow inputWindow) {}

            @Override
            public void cleanup() {}

            @Override
            public TimestampExtractor getTimestampExtractor() {
                return null;}

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {}

            @Override
            public Map<String, Object> getComponentConfiguration() {
                return null;
            }

        };
        else if (boltParamType == ParamType.VALID_INSTANCE)  {
            return new IStatefulWindowedBolt() {
                @Override
                public boolean isPersistent() {
                    return true;
                }

                @Override
                public void initState(State state) {}

                @Override
                public void preCommit(long txid) {}

                @Override
                public void prePrepare(long txid) {}

                @Override
                public void preRollback() {}

                @Override
                public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

                @Override
                public void execute(TupleWindow inputWindow) {}

                @Override
                public void cleanup() {}

                @Override
                public TimestampExtractor getTimestampExtractor() {
                    return null;}

                @Override
                public void declareOutputFields(OutputFieldsDeclarer declarer) {}

                @Override
                public Map<String, Object> getComponentConfiguration() {
                    return null;}


                private void writeObject(java.io.ObjectOutputStream stream) {}
            };
        }
        else return null;
    }



}

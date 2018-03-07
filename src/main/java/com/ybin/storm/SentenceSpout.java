package com.ybin.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author yuebing
 * @version 1.0 2017/9/4
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private String[] sentence = {
            "I have a dog",
            "The dog is big dog",
            "Tt's name is huahua",
            "It is a brave dog"
    };

    private int index = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
    }
    @Override
    public void nextTuple() {
        if (index < sentence.length) {
            this.outputCollector.emit(new Values(sentence[index]));
            index++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}

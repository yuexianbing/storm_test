package com.ybin.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @author yuebing
 * @version 1.0 2017/9/4
 */
public class WordCountTopology {

    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(WordCountTopology.class);
    private static final String SENTENCE_SPOUT_ID = "sentence_spout";
    private static final String SPLIT_BOLT_ID = "split_bolt";
    private static final String COUNT_BOLT_ID = "count_bolt";
    private static final String REPORT_BOLT_ID = "report_bolt";
    private static final String WORD_COUNT_TOPOLOGY_NAME = "word_count_topology";

    private TopologyBuilder builder = new TopologyBuilder();

    public WordCountTopology () {
        builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout());
        builder.setBolt(SPLIT_BOLT_ID, new SpiltSentenceBolt()).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, new WordCountBolt()).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, new ResultBolt()).globalGrouping(COUNT_BOLT_ID);
    }

    public void runLocal(Config config) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(WORD_COUNT_TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology(WORD_COUNT_TOPOLOGY_NAME);
        cluster.shutdown();
    }

    public void runCluster(String num, Config config) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology(num, config, builder.createTopology());
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
       // config.setNumWorkers(2);
        WordCountTopology topology = new WordCountTopology();
        if (args.length == 0) {
            LOG.info("本地WordCountTopology启动!!");
            topology.runLocal(config);
        } else {
            LOG.info("生产WordCountTopology启动!!");
            topology.runCluster(args[0], config);
        }
    }
}

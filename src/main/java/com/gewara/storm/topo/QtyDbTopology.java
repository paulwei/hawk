/**  
* @Project: hawk
* @Title: PvUvTopology.java
* @Package com.gewara.storm.topo
* @Description: ³Ö¾Ã»¯ÍØÆË
* @author honglin.wei@gewara.com
* @date Mar 6, 2014 5:18:58 PM
* @version V1.0  
*/

package com.gewara.storm.topo;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;
import com.gewara.storm.bolt.base.ReadJsonBolt;
 
public class QtyDbTopology {
 	public static void main(String[] args) throws  Exception {
	    Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);
        QtyDbTopology kafkaUvTopology = new QtyDbTopology();
        StormTopology stormTopology = kafkaUvTopology.buildTopology();
		if (args != null && args.length > 0) {
            String name = args[0];
            config.setDebug(false);
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
	}
	
	   public StormTopology buildTopology() {
		    String spoutId = "qtyDb";
		    String zkRoot="/kafkaStorm";
		    String topic = ConfigProps.TOPIC_TABLE;
		    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, zkRoot, spoutId);
	        kafkaConfig.forceStartOffsetTime(-1);
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("readJson", new ReadJsonBolt()).shuffleGrouping("kafkaSpout");
//			builder.setBolt("qtyFilter", new TableFilterBolt(TableEnum.tradeqty.name())).shuffleGrouping("readJson");
//			builder.setBolt("dbWrite", new DBTicketWriteBolt(1)).shuffleGrouping("qtyFilter"); 
	        return builder.createTopology();
	    }

}

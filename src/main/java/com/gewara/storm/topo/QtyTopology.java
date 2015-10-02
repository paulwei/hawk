/**  
* @Project: hawk
* @Title: PvUvTopology.java
* @Package com.gewara.storm.topo
* @Description: ²âÊÔÊýÁ¿
* @author honglin.wei@gewara.com
* @date Mar 6, 2014 5:18:58 PM
* @version V1.0  
*/

package com.gewara.storm.topo;

import java.util.HashMap;
import java.util.Map;

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
import backtype.storm.tuple.Fields;

import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;
import com.gewara.constant.UVEnum;
import com.gewara.storm.bolt.base.FieldByBolt;
import com.gewara.storm.bolt.base.GroupSumBolt;
import com.gewara.storm.bolt.base.ReadJsonBolt;
import com.gewara.storm.bolt.filter.QueryUvFilterBolt;
 
public class QtyTopology {
 
	public static void main(String[] args) throws  Exception {
	    Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);
        QtyTopology kafkaUvTopology = new QtyTopology();
        StormTopology stormTopology = kafkaUvTopology.buildTopology();
		if (args != null && args.length > 0) {
            String name = args[0];
            config.setDebug(false);
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
	}
	
	   public StormTopology buildTopology() {
		    String spoutId = "qty";
		    String zkRoot="/kafkaStorm";
		    String topic = ConfigProps.TOPIC_USER;
		    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, zkRoot, spoutId);
	        kafkaConfig.forceStartOffsetTime(-1);
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("readJson", new ReadJsonBolt()).shuffleGrouping("spout");
			Map<UVEnum,String> quryFilter = new HashMap();
			quryFilter.put(UVEnum.sn, ConfigProps.SN_UV);
			builder.setBolt("snFilter", new QueryUvFilterBolt(quryFilter)).shuffleGrouping("readJson");
			Fields groupFeilds = new Fields(UVEnum.sn.name(),UVEnum.pkey.name(),UVEnum.uagent.name());
			builder.setBolt("fieldBy", new FieldByBolt(groupFeilds)).shuffleGrouping("snFilter");

			builder.setBolt("qtyBolt", new GroupSumBolt(groupFeilds,"quantity","qtyBolt")).fieldsGrouping("fieldBy", groupFeilds);
//			builder.setBolt("kafkaPrd", new KafkaProducerBolt(TableEnum.tradeqty.name())).globalGrouping("qtyBolt"); 

	        return builder.createTopology();
	    }

}

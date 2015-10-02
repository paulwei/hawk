/**  
* @Project: hawk
* @Title: BehaviorTopology.java
* @Package com.gewara.storm.topo
* @Description: 用户行为（下载，分享）
* @author honglin.wei@gewara.com
* @date Mar 6, 2014 5:18:58 PM
* @version V1.0  
*/

package com.gewara.storm.topo;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import com.gewara.constant.BehaviorEnum;
import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;
import com.gewara.constant.TableEnum;
import com.gewara.storm.bolt.base.FieldByBolt;
import com.gewara.storm.bolt.base.GroupCountBolt;
import com.gewara.storm.bolt.base.KafkaProducerBolt;
import com.gewara.storm.bolt.base.ReadJsonBolt;
import com.gewara.storm.bolt.filter.SnFilterBolt;
import com.gewara.storm.bolt.filter.TimeFilterBolt;
import com.gewara.util.GewaUtil;
 
public class BehaviorTopology {
    private static final Logger logger = LoggerFactory.getLogger(BehaviorTopology.class); 

	public static void main(String[] args) throws  Exception {
	    Config config = new Config();
        BehaviorTopology kafkaUvTopology = new BehaviorTopology();
 		if (args != null && args.length > 0) {
	        StormTopology stormTopology = kafkaUvTopology.buildTopology(args.length>1?args[1]:null);
            String name = args[0];
            config.setDebug(false);
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
	        StormTopology stormTopology = kafkaUvTopology.buildTopology(null);
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
	}
	
	   public StormTopology buildTopology(String fromBegin) throws IOException, InterruptedException, ExecutionException {
		    String spoutId = "behavior";
		    String zkRoot="/kafkaStorm";
		    String topic = ConfigProps.TOPIC_USER;
		    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, zkRoot, spoutId);
	        if(StringUtils.isNotBlank(fromBegin) && "from-beginning".equalsIgnoreCase(fromBegin.trim())){
		        kafkaConfig.forceStartOffsetTime(-2);
		        GewaUtil.getKey("downloadCount",logger);GewaUtil.deleteKey("downloadCount",logger);
		        GewaUtil.getKey("shareCount",logger);GewaUtil.deleteKey("shareCount",logger);
	        }else{
		        kafkaConfig.forceStartOffsetTime(-1);
	        }
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("readJson", new ReadJsonBolt()).shuffleGrouping("spout");
			builder.setBolt("timeFilter", new TimeFilterBolt()).shuffleGrouping("readJson");

			//下载链接数
			builder.setBolt("downloadFilter", new SnFilterBolt(ConfigProps.SN_DOWNLOAD)).shuffleGrouping("timeFilter");
			Fields downloadGrp = new Fields(BehaviorEnum.sn.name(),BehaviorEnum.pkey.name(),BehaviorEnum.uagent.name());
			builder.setBolt("downloadFieldBy", new FieldByBolt(downloadGrp)).shuffleGrouping("downloadFilter");
			builder.setBolt("downloadCount", new GroupCountBolt(downloadGrp,"downloadCount")).fieldsGrouping("downloadFieldBy", downloadGrp);

			//分享链接数
			builder.setBolt("shareFilter", new SnFilterBolt(ConfigProps.SN_SHARE)).shuffleGrouping("timeFilter");
			Fields shareGrp = new Fields(BehaviorEnum.sn.name(),BehaviorEnum.pkey.name(),BehaviorEnum.uagent.name());
			builder.setBolt("shareFieldBy", new FieldByBolt(shareGrp)).shuffleGrouping("shareFilter");
			builder.setBolt("shareCount", new GroupCountBolt(shareGrp,"shareCount")).fieldsGrouping("shareFieldBy", shareGrp);
			
			//kafkaproducer
			builder.setBolt("download", new KafkaProducerBolt(TableEnum.pj_download.name())).globalGrouping("downloadCount"); 
			builder.setBolt("share", new KafkaProducerBolt(TableEnum.pj_share.name())).globalGrouping("shareCount"); 
 
	        return builder.createTopology();
	    }
	   
	   

}

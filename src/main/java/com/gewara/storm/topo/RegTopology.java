/**  
* @Project: hawk
* @Title: RegTopology.java
* @Package com.gewara.storm.topo
* @Description: 用户注册
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
import com.gewara.constant.RegEnum;
import com.gewara.constant.TableEnum;
import com.gewara.storm.bolt.base.FieldByBolt;
import com.gewara.storm.bolt.base.GroupCountBolt;
import com.gewara.storm.bolt.base.KafkaProducerBolt;
import com.gewara.storm.bolt.base.ReadJsonBolt;
import com.gewara.storm.bolt.filter.NeFilterBolt;
import com.gewara.storm.bolt.filter.NotNvlFilterBolt;
import com.gewara.storm.bolt.filter.SnFilterBolt;
import com.gewara.storm.bolt.filter.TimeFilterBolt;
import com.gewara.util.GewaUtil;
 
public class RegTopology {
    private static final Logger logger = LoggerFactory.getLogger(RegTopology.class); 

	public static void main(String[] args) throws  Exception {
	    Config config = new Config();
        RegTopology kafkaUvTopology = new RegTopology();
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
		    String spoutId = "reg";
		    String zkRoot="/kafkaStorm";
		    String topic = ConfigProps.TOPIC_USER;
		    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, zkRoot, spoutId);
	        if(StringUtils.isNotBlank(fromBegin) && "from-beginning".equalsIgnoreCase(fromBegin.trim())){
		        kafkaConfig.forceStartOffsetTime(-2);
		        GewaUtil.getKey("indiRegCount",logger);GewaUtil.deleteKey("indiRegCount",logger);
		        GewaUtil.getKey("dirRegCount",logger);GewaUtil.deleteKey("dirRegCount",logger);
	        }else{
		        kafkaConfig.forceStartOffsetTime(-1);
	        }
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("readJson", new ReadJsonBolt()).shuffleGrouping("spout");
			builder.setBolt("timeFilter", new TimeFilterBolt()).shuffleGrouping("readJson");
			builder.setBolt("regFilter", new SnFilterBolt(ConfigProps.SN_REG)).shuffleGrouping("timeFilter");

			//间接注册用户数
			builder.setBolt("neFilter", new NeFilterBolt(RegEnum.fpkey.name(),RegEnum.cpkey.name())).shuffleGrouping("regFilter");
			Fields indiGrp = new Fields(RegEnum.sn.name(),RegEnum.fpkey.name(),BehaviorEnum.uagent.name());
			builder.setBolt("indiFieldBy", new FieldByBolt(indiGrp)).shuffleGrouping("neFilter");
			builder.setBolt("indiRegCount", new GroupCountBolt(indiGrp,"indiRegCount")).fieldsGrouping("indiFieldBy", indiGrp);

			//直接注册用户数
			builder.setBolt("notNvlFilter", new NotNvlFilterBolt(RegEnum.cpkey.name())).shuffleGrouping("regFilter");
			Fields dirGrp = new Fields(RegEnum.sn.name(),RegEnum.cpkey.name(),BehaviorEnum.uagent.name());
			builder.setBolt("dirFieldBy", new FieldByBolt(dirGrp)).shuffleGrouping("notNvlFilter");
			builder.setBolt("dirRegCount", new GroupCountBolt(dirGrp,"dirRegCount")).fieldsGrouping("dirFieldBy", dirGrp);
			
			//kafkaproducer
			builder.setBolt("indiReg", new KafkaProducerBolt(TableEnum.pj_reguserindi.name())).globalGrouping("indiRegCount"); 
			builder.setBolt("dirReg", new KafkaProducerBolt(TableEnum.pj_reguserdir.name())).globalGrouping("dirRegCount"); 
 
	        return builder.createTopology();
	    }

}

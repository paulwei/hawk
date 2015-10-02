/**  
* @Project: hawk
* @Title: ConsumeTopology.java
* @Package com.gewara.storm.topo
* @Description: 用户消费
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

import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;
import com.gewara.constant.ConsumeEnum;
import com.gewara.constant.TableEnum;
import com.gewara.storm.bolt.base.FieldByBolt;
import com.gewara.storm.bolt.base.GroupDistBolt;
import com.gewara.storm.bolt.base.GroupSumBolt;
import com.gewara.storm.bolt.base.KafkaProducerBolt;
import com.gewara.storm.bolt.base.ReadJsonBolt;
import com.gewara.storm.bolt.filter.MappingFilterBolt;
import com.gewara.storm.bolt.filter.NeFilterBolt;
import com.gewara.storm.bolt.filter.NotNvlFilterBolt;
import com.gewara.storm.bolt.filter.SnFilterBolt;
import com.gewara.storm.bolt.filter.TimeFilterBolt;
import com.gewara.util.GewaUtil;
 
public class ConsumeTopology {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeTopology.class); 

	public static void main(String[] args) throws  Exception {
	    Config config = new Config();
        ConsumeTopology kafkaUvTopology = new ConsumeTopology();
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
		    String spoutId = "consume";
		    String zkRoot="/kafkaStorm";
		    String topic = ConfigProps.TOPIC_USER;
		    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, zkRoot, spoutId);
	        if(StringUtils.isNotBlank(fromBegin) && "from-beginning".equalsIgnoreCase(fromBegin.trim())){
		        kafkaConfig.forceStartOffsetTime(-2);
		        GewaUtil.getKey("indiConsumerCount",logger);GewaUtil.deleteKey("indiConsumerCount",logger);
		        GewaUtil.getKey("indiQtySum",logger);GewaUtil.deleteKey("indiQtySum",logger);
		        GewaUtil.getKey("dirConsumerCount",logger);GewaUtil.deleteKey("dirConsumerCount",logger);
		        GewaUtil.getKey("dirQtySum",logger);GewaUtil.deleteKey("dirQtySum",logger);
	        }else{
		        kafkaConfig.forceStartOffsetTime(-1);
	        }	       
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("readJson", new ReadJsonBolt()).shuffleGrouping("spout");
			builder.setBolt("snFilter", new SnFilterBolt(ConfigProps.SN_CONSUME)).shuffleGrouping("readJson");
			builder.setBolt("timeFilter", new TimeFilterBolt()).shuffleGrouping("snFilter");
			//间接消费用户数
			builder.setBolt("neFilter", new NeFilterBolt(ConsumeEnum.fpkey.name(),ConsumeEnum.cpkey.name())).shuffleGrouping("timeFilter");
			builder.setBolt("indiMappingFilter", new MappingFilterBolt(ConsumeEnum.type.name(),ConsumeEnum.ordertype.name())).shuffleGrouping("neFilter");
			Fields indiGrp = new Fields(ConsumeEnum.sn.name(),ConsumeEnum.fpkey.name(),ConsumeEnum.uagent.name(),ConsumeEnum.foro.name(),ConsumeEnum.ordertype.name());
			builder.setBolt("indiFieldBy", new FieldByBolt(indiGrp)).shuffleGrouping("indiMappingFilter");
			builder.setBolt("indiConsumerCount", new GroupDistBolt(indiGrp,ConsumeEnum.uid.name(),"indiConsumerCount")).fieldsGrouping("indiFieldBy", indiGrp);
			builder.setBolt("indiQtySum", new GroupSumBolt(indiGrp,ConsumeEnum.tc.name(),"indiQtySum")).fieldsGrouping("indiFieldBy", indiGrp);
			//直接消费用户数
			builder.setBolt("notNvlFilter", new NotNvlFilterBolt("cpkey")).shuffleGrouping("timeFilter");
			builder.setBolt("dirMappingFilter", new MappingFilterBolt(ConsumeEnum.type.name(),ConsumeEnum.ordertype.name())).shuffleGrouping("notNvlFilter");
			Fields dirGrp = new Fields(ConsumeEnum.sn.name(),ConsumeEnum.cpkey.name(),ConsumeEnum.uagent.name(),ConsumeEnum.foro.name(),ConsumeEnum.ordertype.name());
			builder.setBolt("dirFieldBy", new FieldByBolt(dirGrp)).shuffleGrouping("dirMappingFilter");
			builder.setBolt("dirConsumerCount", new GroupDistBolt(dirGrp,ConsumeEnum.uid.name(),"dirConsumerCount")).fieldsGrouping("dirFieldBy", dirGrp);
			builder.setBolt("dirQtySum", new GroupSumBolt(dirGrp,ConsumeEnum.tc.name(),"dirQtySum")).fieldsGrouping("dirFieldBy", dirGrp);
			//kafkaproducer
			builder.setBolt("indiConsumer", new KafkaProducerBolt(TableEnum.pj_conuserindi.name())).globalGrouping("indiConsumerCount"); 
			builder.setBolt("indiQty", new KafkaProducerBolt(TableEnum.pj_conqtyindi.name())).globalGrouping("indiQtySum"); 
			builder.setBolt("dirConsumer", new KafkaProducerBolt(TableEnum.pj_conuserdir.name())).globalGrouping("dirConsumerCount"); 
			builder.setBolt("dirQty", new KafkaProducerBolt(TableEnum.pj_conqtydir.name())).globalGrouping("dirQtySum"); 
            //
	        return builder.createTopology();
	    }

}

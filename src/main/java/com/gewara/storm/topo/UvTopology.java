/**  
* @Project: hawk
* @Title: UvTopology.java
* @Package com.gewara.storm.topo
* @Description: UVÍØÆË
* @author honglin.wei@gewara.com
* @date Mar 6, 2014 5:18:58 PM
* @version V1.0  
*/

package com.gewara.storm.topo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

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
import com.gewara.constant.TableEnum;
import com.gewara.constant.UVEnum;
import com.gewara.storm.bolt.base.FieldByBolt;
import com.gewara.storm.bolt.base.GroupDistBolt;
import com.gewara.storm.bolt.base.KafkaProducerBolt;
import com.gewara.storm.bolt.base.ReadJsonBolt;
import com.gewara.storm.bolt.filter.RefFilterBolt;
import com.gewara.storm.bolt.filter.SnFilterBolt;
import com.gewara.storm.bolt.filter.TimeFilterBolt;
import com.gewara.util.BloomFilter;
import com.gewara.util.GewaUtil;
 
public class UvTopology {
    private static final Logger logger = LoggerFactory.getLogger(UvTopology.class); 

	public static void main(String[] args) throws  Exception {
 	    Config config = new Config();
        UvTopology kafkaUvTopology = new UvTopology();
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
		    String spoutId = "uv";
		    String zkRoot="/kafkaStorm";
		    String topic = ConfigProps.TOPIC_USER;
		    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, zkRoot, spoutId);
	        if(StringUtils.isNotBlank(fromBegin) && "from-beginning".equalsIgnoreCase(fromBegin.trim())){
		        kafkaConfig.forceStartOffsetTime(-2);
		        logger.info("topo="+spoutId+" fromBegin");
		        GewaUtil.getKey("uv",logger);GewaUtil.deleteKey("uv",logger);
	        }else{
		        kafkaConfig.forceStartOffsetTime(-1);
	        }
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("readJson", new ReadJsonBolt()).shuffleGrouping("spout");
			builder.setBolt("snFilter", new SnFilterBolt(ConfigProps.SN_UV)).shuffleGrouping("readJson");
			builder.setBolt("timeFilter", new TimeFilterBolt()).shuffleGrouping("snFilter");
			builder.setBolt("refFilter", new RefFilterBolt()).shuffleGrouping("timeFilter");
			
			Fields groupFeilds = new Fields(UVEnum.sn.name(),UVEnum.pkey.name(),UVEnum.reftype.name(),UVEnum.uagent.name());
			builder.setBolt("fieldBy", new FieldByBolt(groupFeilds)).shuffleGrouping("refFilter");

			builder.setBolt("uv", new GroupDistBolt(groupFeilds,UVEnum.uasn.name(),"uv")).fieldsGrouping("fieldBy", groupFeilds);
			builder.setBolt("kafkaPrd", new KafkaProducerBolt(TableEnum.pj_uv.name())).globalGrouping("uv"); 

	        return builder.createTopology();
	    }
	   
		public  static void deleteKey(String key) throws IOException, InterruptedException, ExecutionException{
			String host = ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
			Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
			MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress(host,port));
			OperationFuture<Boolean> operate = memcachedClient.delete(key);
			logger.debug("delete memcache key="+key+",flag="+operate.get());
			System.out.println("delete memcache key="+key+",flag="+operate.get());
		}
		public  static void getKey(String key) throws IOException, InterruptedException, ExecutionException{
			String host = ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
			Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
			MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress(host,port));
			Map<String, Object> m = (Map<String, Object>)memcachedClient.get(key);
			if(m==null || m.isEmpty()) {
				logger.debug("get memcache key="+key+" is empty");
				System.out.println("get memcache key="+key+" is empty");
				return ;
			}
			for(String k:m.keySet()){
				Object o = m.get(k);
				if( o instanceof BloomFilter){
					logger.debug("get memcache key="+key+"["+k+"="+((BloomFilter)o).getSize()+"]");
					System.out.println("get memcache key="+key+"["+k+"="+((BloomFilter)o).getSize()+"]");
				}else{
					logger.debug("get memcache key="+key+"["+k+"="+o.toString()+"]");
					System.out.println("get memcache key="+key+"["+k+"="+o.toString()+"]");
				}
			}
		}


}

/**  
* @Project: hawk
* @Title: TridentPv.java
* @Package com.gewara.storm.trident
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Apr 17, 2014 7:27:19 PM
* @version V1.0  
*/

package com.gewara.test;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;

public class TridentPv {
	/**
	 * @param args
	 * @throws Exception 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, Exception {
		Config config=new Config();
		config.setMaxSpoutPending(20);
		
		if(args.length==0){
			config.setMaxTaskParallelism(5);
			LocalCluster localCluster=new LocalCluster();
			LocalDRPC localDRPC=new LocalDRPC();
			StormTopology stormTopology = buildTopology(localDRPC);
			localCluster.submitTopology("wordcount", config, stormTopology);
			for(int i=0;i<100;i++){
				System.out.println("DRPC RESULT:"+localDRPC.execute("words", "cat the dog jumped"));
				Thread.sleep(1000);
			}
		}else{
			config.setNumWorkers(3);
			StormTopology stormTopology = buildTopology(null);
			StormSubmitter.submitTopology(args[0], config, stormTopology);
		}
	}
	
	public static StormTopology buildTopology(LocalDRPC localDRPC){
	    BrokerHosts brokerHosts =  new ZkHosts(ConfigFactory.getConfigProps().getString(ConfigProps.KEY_ZOOKEEPER_KAFKA));
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, ConfigProps.TOPIC_USER, "pv");  
		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig); 
		
		TridentTopology topology=new TridentTopology();
		TridentState tridentState = topology.newStream("spout", kafkaSpout)
			.parallelismHint(16)//²¢ÐÐ
			.each(new Fields("msg"), new Split(), new Fields("item"))
			//sentenceÊäÈë 
			.groupBy(new Fields("word"))
			.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
			.parallelismHint(6);
		
		topology.newDRPCStream("words", localDRPC)
			.each(new Fields("args"), new Split(), new Fields("word"))
			.groupBy(new Fields("word"))
			.stateQuery(tridentState, new Fields("word"), new MapGet(), new Fields("count"))
			.each(new Fields("count"), new FilterNull())
			.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		
		return topology.build();
	}
}

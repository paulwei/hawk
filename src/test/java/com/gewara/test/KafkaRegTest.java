/**  
* @Project: hawk
* @Title: KafkaStormProducer.java
* @Package com.gewara.storm.util
* @Description: kafaka产生数据测试
* @author honglin.wei@gewara.com
* @date Mar 12, 2014 5:06:13 PM
* @version V1.0  
*/

package com.gewara.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gewara.constant.ConfigProps;
import com.gewara.util.DateUtil;
import com.gewara.util.JsonUtils;

public class KafkaRegTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaRegTest.class); 

//    private static String zooKeeper = "192.168.2.183:2181,192.168.2.108:2181,192.168.2.182:2181";
    private static String broker ="192.168.8.110:9092,192.168.8.111:9092";
	private static String[] pkey=new String[]{"1013","1022","1028","1018"};
	private static String[] uagent=new String[]{"web","wap","app"};
	public static void main(String[] args) throws InterruptedException, IOException {
		Properties props = new Properties();
		props.put("metadata.broker.list", broker);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        final Map<String,Integer> indiCountMap  = new HashMap<String,Integer>();
        final Map<String,Integer> dirCountMap  = new HashMap<String,Integer>();
 
                  for(int i=0;i<10;i++){
                	    Random r = new Random();
	            		Map map  = new HashMap();
	            		map.put("sn", "101000000002");
	            		map.put("fpkey", pkey[r.nextInt(4)]);
	            		map.put("cpkey", pkey[r.nextInt(4)]);
	            		map.put("uagent", uagent[r.nextInt(3)]);
	            		map.put("timestamp",DateUtil.getCurFullTimestamp());
	            		logger.debug("reg map:"+map);
	            		String indiGroupValueKey = (String)map.get("sn")+(String)map.get("fpkey")+(String)map.get("uagent");
	            		Integer indicount = indiCountMap.get(indiGroupValueKey);
	            		if(indicount!=null){
	            			indicount = indicount+1;
	            			indiCountMap.put(indiGroupValueKey, indicount);
	            		}else{
	            			indiCountMap.put(indiGroupValueKey, 1);
	            		}
	            		String dirGroupValueKey = (String)map.get("sn")+(String)map.get("cpkey")+(String)map.get("uagent");
	            		Integer dircount = dirCountMap.get(dirGroupValueKey);
	            		if(dircount!=null){
	            			dircount = dircount+1;
	            			dirCountMap.put(dirGroupValueKey, dircount);
	            		}else{
	            			dirCountMap.put(dirGroupValueKey, 1);
	            		}
 		            	String json = JsonUtils.writeObjectToJson(map);
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(ConfigProps.TOPIC_USER,"reg"+i, json);
		                producer.send(data);
		                System.out.println(data.key()+",json="+json);
		                Thread.sleep(100);
                  }
          	logger.debug("reg indiCountMap:"+indiCountMap);
          	logger.debug("reg dirCountMap:"+dirCountMap);
            System.out.println("Reg indiCountMap:%%%%%%%%%%%%%%%%%%%%%"+indiCountMap);
            System.out.println("Reg dirCountMap:%%%%%%%%%%%%%%%%%%%%%"+dirCountMap);
 
	}
	
	public static void indi(){
		
	}
}

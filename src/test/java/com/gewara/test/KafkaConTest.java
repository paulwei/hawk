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

import com.gewara.constant.ConfigProps;
import com.gewara.util.JsonUtils;

public class KafkaConTest {
//    private static String zooKeeper = "192.168.2.183:2181,192.168.2.108:2181,192.168.2.182:2181";
    private static String broker ="192.168.8.110:9092,192.168.8.111:9092";
	private static String[] pkey=new String[]{"1013","1022","1028","1018"};
	private static String[] uagent=new String[]{"web","wap","app"};
	private static String[] foro=new String[]{"first","old"};
	private static String[] type=new String[]{"ticket","drama","goods"};
	private static Integer[] tc=new Integer[]{2,8,3,5,1};
	private static String[] uid=new String[]{"40618364","44578622","44188945","43872760","44580078","44580481","44580629","40308733","38818714"};
//	private static String[] sem=new String[]{"utm_source=sosocom&utmcmd=hzcpc","","utm_source=baidu.com&utm_medim=shcpc"};
//	private static String[] ref=new String[]{"www.gewara.com","www.douban.com","www.baidu.com","www.taobao.com","www.soso.com","www.so.com"};
	public static void main(String[] args) throws InterruptedException, IOException {
		Properties props = new Properties();
		props.put("metadata.broker.list", broker);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
                  for(int i=0;i<68;i++){
                	    Random r = new Random();
	            		Map map  = new HashMap();
	            		map.put("sn", "101000000003");
	            		map.put("fpkey" , pkey[r.nextInt(4)]);
	            		map.put("cpkey" , pkey[r.nextInt(4)]);
	            		map.put("uagent", uagent[r.nextInt(3)]);
//	            		map.put("sem", sem[r.nextInt(3)]);
//	            		map.put("ref", ref[r.nextInt(6)]);
	            		map.put("uid", uid[r.nextInt(9)]);
	            		map.put("foro", foro[r.nextInt(2)]);
	            		map.put("type", type[r.nextInt(3)]);
	            		map.put("tc", tc[r.nextInt(5)]);
	            		map.put("timestamp",System.currentTimeMillis()+"");
 		            	String json = JsonUtils.writeObjectToJson(map);
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(ConfigProps.TOPIC_USER,"reg"+i, json);
		                producer.send(data);
		                System.out.println(data.key()+",json="+json);
		                Thread.sleep(1000);
                  }
	}
	
	public static void indi(){
		
	}
}

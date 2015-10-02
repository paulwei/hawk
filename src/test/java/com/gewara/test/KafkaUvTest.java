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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.gewara.constant.ConfigProps;
import com.gewara.util.BloomFilter;
import com.gewara.util.JsonUtils;

public class KafkaUvTest {
//    private static String zooKeeper = "192.168.2.183:2181,192.168.2.108:2181,192.168.2.182:2181";
    private static String broker ="192.168.8.110:9092,192.168.8.111:9092";
	private static String[] pkey=new String[]{"p111","p222","p333"};
	private static String[] uagent=new String[]{"pc","wap","app"};
	private static String[] uasn=new String[]{"1397212421483.856821","1397212421483.856822"};
	private static String[] foro=new String[]{"first","old"};
	private static Integer[] tc=new Integer[]{2,8,3,5,1};
	private static String[] uid=new String[]{"u001","u002","u003","u004","u005","u006","u007","u008"};
	private static String[] sem=new String[]{"utm_source=sosocom&utmcmd=hzcpc","","utm_source=baidu.com&utm_medim=shcpc"};
	private static String[] ref=new String[]{"www.gewara.com","www.douban.com","www.baidu.com","www.taobao.com","www.soso.com","www.so.com"};
 	private static ArrayList<String> sentences = new  ArrayList<String>();
	public static void main(String[] args) throws InterruptedException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File("src/main/resources/browser.txt")));
		String line =reader.readLine();
		while(line!=null){
			sentences.add(line);
			line =reader.readLine();
		}
		Properties props = new Properties();
		props.put("metadata.broker.list", broker);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        final Map<String,Integer> countMap  = new HashMap<String,Integer>();
    	final Map<String, BloomFilter> countMapUV=new ConcurrentHashMap<String, BloomFilter>();
    	final Map<String, Integer> sumMap=new HashMap<String, Integer>();

             for (int i =0;i<70;i++) {
//	            		UVEntry uvEntry = new UVEntry(split[0],split[2],split[1],split[3],DateUtil.getCurFullTimestamp());
//	            		String groupValueKey = uvEntry.getSn()+","+uvEntry.getPkey()+","+uvEntry.getUagent()+",referral";
//	            		uvEntry.setUasn(uvEntry.getUasn()+new Random());
            	        Random r = new Random();
	            		Map map  = new HashMap();
	            		map.put("sn", "101000000001");
	            		map.put("pkey",  "p333"/*pkey[r.nextInt(3)]*/);
	            		map.put("uagent", "wap"/*uagent[r.nextInt(3)]*/);
	            		map.put("uasn", "uasn"+i);
	            		map.put("sem", "utm_source=sosocom&utmcmd=hzcpc"/*sem[r.nextInt(3)]*/);
	            		map.put("ref", "www.gewara.com"/*ref[r.nextInt(6)]*/);
	            		String groupValueKey = (String)map.get("sn")+","+(String)map.get("pkey")+","+(String)map.get("uagent");
	            		Integer count = countMap.get(groupValueKey);
	            		if(count!=null){
	            			count = count+1;
	            			countMap.put(groupValueKey, count);
	            		}else{
	            			countMap.put(groupValueKey, 1);
	            		}
	            		if(countMapUV.get(groupValueKey)!=null){
	            			countMapUV.get(groupValueKey).add((String)map.get("uasn"));
	            		}else{
	            			BloomFilter filter = new BloomFilter();
	            			filter.add((String)map.get("uasn"));
	            			countMapUV.put(groupValueKey, filter);
	            		}
 		            	String json = JsonUtils.writeObjectToJson(map);
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(ConfigProps.TOPIC_USER,"msg"+(i), json);
		                producer.send(data);
		                System.out.println(data.key()+",json="+json);
		                Thread.sleep(1000);
            }
            System.out.println("countMap:%%%%%%%%%%%%%%%%%%%%%"+countMap);
        	System.out.println("countMapUV:********************");
        	for(String key:countMapUV.keySet()){
            	System.out.println(key+":uv:"+countMapUV.get(key).getSize());
        	}
            System.out.println("sumMap:&&&&&&&&&&&&&&&&&&"+sumMap);

	}
	
/*	public Map getMap(){
		Map map = new HashMap();
		map.put("sn", "101000000001");
		map.put("pkey", "1030");
		map.put("sem", "232105481.1397829537.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic|utmctr=%E6%A0%BC%E7%93%A6%E6%8B%89%20%E9%87%8D%E5%BA%86");
	}*/
}

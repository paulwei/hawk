/**  
* @Project: hawk
* @Title: ConfigFactory.java
* @Package com.gewara.constant
* @Description: ≈‰÷√≥£¡ø
* @author honglin.wei@gewara.com
* @date Apr 15, 2014 11:39:24 PM
* @version V1.0  
*/

package com.gewara.constant;

import java.util.Map;
 
public class ConfigFactory {
	public static ConfigProps local;
	public static ConfigProps remote;
	static{
		//local
		local = new ConfigProps();
		local.put(ConfigProps.KEY_MEMCACHE_PORT, "11211");
		local.put(ConfigProps.KEY_MEMCACHE_HOST, "192.168.8.111");
		local.put(ConfigProps.KEY_BROKER_KAFKA, "192.168.8.110:9092,192.168.8.111:9092");
		local.put(ConfigProps.KEY_ZOOKEEPER_KAFKA, "192.168.8.101:2181,192.168.8.102:2181,192.168.8.103:2181/kafka");
		//Remote
		remote = new ConfigProps();
		remote.put(ConfigProps.KEY_MEMCACHE_PORT,"11211");
		remote.put(ConfigProps.KEY_MEMCACHE_HOST, "local15");
		remote.put(ConfigProps.KEY_BROKER_KAFKA, "local14:9092,local15:9092");
		remote.put(ConfigProps.KEY_ZOOKEEPER_KAFKA, "local31:2181,local32:2181,local33:2181,local34:2181,local35:2181/kafka");
	}
	public static ConfigProps getConfigProps(){
		Map<String,String> hostMap = ConfigProps.getServerAddrMap();
		for(String ip:hostMap.keySet()){
			if(ip.startsWith("172.22.1")){
				return remote;
			}
		}
		return local;
	}
	
}

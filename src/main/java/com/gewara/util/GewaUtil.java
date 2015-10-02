/**  
* @Project: hawk
* @Title: GewaUtil.java
* @Package com.gewara.util
* @Description: π§æﬂ¿‡
* @author honglin.wei@gewara.com
* @date Apr 21, 2014 10:30:01 AM
* @version V1.0  
*/

package com.gewara.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.slf4j.Logger;

import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;

 
public class GewaUtil {
	public  static void deleteKey(String key,Logger logger) throws IOException, InterruptedException, ExecutionException{
		String host = ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
		Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
		MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress(host,port));
		OperationFuture<Boolean> operate = memcachedClient.delete(key);
		logger.debug("delete memcache key="+key+",flag="+operate.get());
		System.out.println("delete memcache key="+key+",flag="+operate.get());
	}
	public  static void getKey(String key,Logger logger) throws IOException, InterruptedException, ExecutionException{
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

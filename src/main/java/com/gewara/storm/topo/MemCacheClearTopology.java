/**  
* @Project: hawk
* @Title: MemCacheClearTopology.java
* @Package com.gewara.storm.topo
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Apr 15, 2014 12:14:43 PM
* @version V1.0  
*/

package com.gewara.storm.topo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gewara.util.GewaUtil;
 
public class MemCacheClearTopology {
    private static final Logger logger = LoggerFactory.getLogger(MemCacheClearTopology.class); 

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		if(args!=null&&args.length>1){
			List<String> keys  = new ArrayList();
			//uv
			keys.add("uv");
			//reg
			keys.add("indiRegCount");
			keys.add("dirRegCount");
			//consume
			keys.add("indiConsumerCount");
			keys.add("indiQtySum");
			keys.add("dirConsumerCount");
			keys.add("dirQtySum");
			//behaivor
			keys.add("downloadCount");
			keys.add("shareCount");
            if(args[0].equals("delete")){
            	if(args[1].equals("all")){
                   	for(String key:keys){
                   		GewaUtil.getKey(key,logger);
                   		GewaUtil.deleteKey(key,logger);
                	}
            	}
            	else{
            		GewaUtil.getKey(args[1],logger);
            		GewaUtil.deleteKey(args[1],logger);
            	}
            }else if(args[0].equals("get")){
            	if(args[1].equals("all")){
                   	for(String key:keys){
                   		GewaUtil.getKey(key,logger);
                	}
            	}
            	else{
            		GewaUtil.getKey(args[1],logger);
            	}
            }
		}else{
			logger.debug(" memcache {operate,key} must not null");
			System.out.println(" memcache {operate,key} must not null");
		}
	}

}

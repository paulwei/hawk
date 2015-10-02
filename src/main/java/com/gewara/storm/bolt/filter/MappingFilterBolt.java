/**  
* @Project: hawk
* @Title: SnFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: ·Ç¿Õ¹ýÂËÆ÷
* @author honglin.wei@gewara.com
* @date Mar 21, 2014 10:52:01 AM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class MappingFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 384007971596058711L;
    private String oriKey;
    private String destkey;
    
	public MappingFilterBolt(String oriKey,String destkey) {
		this.oriKey = oriKey;
		this.destkey = destkey;
	}

	public boolean accept(Map map) {
        if(StringUtils.isNotBlank(oriKey)){
        	map.put(destkey, map.get(oriKey));
         }
		return true;
	}

}

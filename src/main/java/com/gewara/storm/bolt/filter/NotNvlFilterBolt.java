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

public class NotNvlFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 384007971596058711L;
    private String key;
    
	public NotNvlFilterBolt(String key) {
		this.key = key;
	}

	public boolean accept(Map map) {
        if(map!=null &&  (map.get(key)==null || StringUtils.isBlank((String)map.get(key)) || "NVL".equalsIgnoreCase((String)map.get(key)))){
        	return false;
        }
		return true;
	}

}

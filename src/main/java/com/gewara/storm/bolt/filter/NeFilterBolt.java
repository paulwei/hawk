/**  
* @Project: hawk
* @Title: SnFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: ²»µÈ¹ýÂËÆ÷
* @author honglin.wei@gewara.com
* @date Mar 21, 2014 10:52:01 AM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

public class NeFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 384007971596058711L;
    private String key1;
    private String key2;
    
	public NeFilterBolt(String key1,String key2) {
		this.key1 = key1;
		this.key2 = key2;
	}

	public boolean accept(Map map) {
        if(map!=null &&  map.get(key1)!=null && map.get(key2)!=null && map.get(key1).equals(map.get(key2))){
        	return false;
        }
		return true;
	}

}

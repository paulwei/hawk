/**  
* @Project: hawk
* @Title: SnFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: 系统分配过滤器
* @author honglin.wei@gewara.com
* @date Mar 21, 2014 10:52:01 AM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.gewara.constant.UVEnum;

public class SnFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 384007971596058711L;
    private String sn;
    
	public SnFilterBolt(String sn) {
		this.sn = sn;
	}

	public boolean accept(Map map) {
        if(map!=null && StringUtils.isNotBlank(sn) && sn.equals(map.get(UVEnum.sn.name()))){
        	return true;
        }
		return false;
	}

}

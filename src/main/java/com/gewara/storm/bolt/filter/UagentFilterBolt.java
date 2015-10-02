/**  
* @Project: hawk
* @Title: PcFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: PC¹ýÂËÆ÷
* @author honglin.wei@gewara.com
* @date Mar 19, 2014 3:21:29 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.gewara.constant.UVEnum;

public class UagentFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = -7377488973328039441L;
	private String uagent;
	
	public UagentFilterBolt(String uagent) {
		this.uagent = uagent;
	}

	@Override
	public boolean accept(Map map) {
        if(map!=null && StringUtils.isNotBlank(uagent) && uagent.equals(map.get(UVEnum.uagent.name()))){
        	return true;
        }
		return false;
	}

}

/**  
* @Project: hawk
* @Title: UvFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: UVÍ³Ò»¹ýÂËÆ÷
* @author honglin.wei@gewara.com
* @date Apr 2, 2014 3:21:21 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.gewara.constant.RegEnum;

public class QueryRegFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = -8585703462073495692L;
	private Map<RegEnum,String> quryFilter;
	public QueryRegFilterBolt(Map<RegEnum, String> quryFilter) {
		this.quryFilter = quryFilter;
	}

	@Override
	public boolean accept(Map map) {
		boolean flag = true;
        if(map!=null && quryFilter!=null){
        	for(RegEnum uv:quryFilter.keySet()){
        		 if(StringUtils.equalsIgnoreCase(quryFilter.get(uv), map.get(uv.name())!=null?map.get(uv.name()).toString():"")){
        			 flag = flag && Boolean.TRUE;
        		 }else{
        			 flag = flag && Boolean.FALSE;
        		 }
        	}
        }
	return flag;
	}

}

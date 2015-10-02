/**  
* @Project: hawk
* @Title: SnFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: Ê±¼ä¹ıÂËÆ÷
* @author honglin.wei@gewara.com
* @date Mar 21, 2014 10:52:01 AM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.gewara.util.DateUtil;

public class TimeFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 384007971596058711L;
	public boolean accept(Map map) {
	   long currentDayBegin =  DateUtil.getCurTruncTimestamp().getTime();
       Object time  = map.get("timestamp");
       long emittTime = 0L;
       if(time!=null&&StringUtils.isNotBlank(time.toString().trim())&&StringUtils.isNumeric(time.toString().trim())){
    	   emittTime = new Long(time.toString().trim());
    	   if(emittTime>currentDayBegin){
    			return true;
    	   }
       }
       return false;
	}

}
